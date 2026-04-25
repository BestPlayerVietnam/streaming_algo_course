package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Формат SSTable на диске:
//
//   [Data Block 1]
//   [Data Block 2]
//   ...
//   [Data Block N]
//   [Index Block]
//   [Footer (16 байт)]
//
// Внутри Data Block — записи подряд: [key_len: uvarint][key][val_len: uvarint][val].
// Внутри Index Block — записи подряд: [first_key_len: uvarint][first_key][offset: uint64 BE][length: uint32 BE].
// Footer всегда 16 байт в самом конце: [index_offset: uint64 BE][index_length: uint32 BE][magic: uint32 BE].
//
// Ключи записываются в строго возрастающем порядке (это инвариант, нарушение -> ошибка Add).

// Ошибки пакета.
var (
	ErrNotImplemented = errors.New("sstable: функция не реализована")
	ErrUnorderedKeys  = errors.New("sstable: ключи должны быть строго возрастающими")
	ErrCorruptFooter  = errors.New("sstable: повреждён footer (неверный magic или размер)")
	ErrCorruptBlock   = errors.New("sstable: повреждённый блок данных")
	ErrCorruptIndex   = errors.New("sstable: повреждённый index block")
	ErrIteratorClosed = errors.New("sstable: итератор закрыт")
)

// Константы формата. Менять опасно — старые SSTable станут нечитаемыми.
const (
	// targetBlockSize — целевой размер data block в байтах. Когда буфер блока
	// при записи достигает этого размера, мы закрываем блок. 4 KiB — типичный
	// размер страницы файловой системы, минимизирует read amplification.
	targetBlockSize = 4 * 1024

	// footerSize — фиксированная длина footer в байтах:
	//   index_offset (8) + index_length (4) + magic (4).
	footerSize = 16

	// magicNumber — sanity check, что файл действительно SSTable.
	// 0x5354424C — ASCII "STBL".
	magicNumber uint32 = 0x5354424C
)

// indexEntry — одна запись в Index Block: ссылка на data block.
// Хранится в памяти Reader'а, по нему ищем нужный блок бинпоиском.
type indexEntry struct {
	firstKey []byte // первый ключ блока (нужен для бинпоиска)
	offset   uint64 // смещение блока от начала файла
	length   uint32 // длина блока в байтах
}

// Writer пишет отсортированные пары key/value в io.Writer.
// Использование: NewWriter -> Add(...) много раз -> Close.
// Ключи должны добавляться в строго возрастающем порядке.
type Writer struct {
	w       io.Writer
	bw      *bufio.Writer // буферизация — снижает количество мелких write() syscalls
	written uint64        // сколько байт уже записано во w (нужно для смещений в индексе)

	curBlock     bytes.Buffer // буфер текущего открытого блока
	curBlockKey0 []byte       // первый ключ текущего блока (запоминаем для индекса)

	prevKey []byte // последний добавленный ключ (для проверки порядка)

	index  []indexEntry // накопленный индекс блоков
	closed bool
}

// NewWriter создаёт Writer поверх io.Writer.
// Writer не закрывает w сам; вызывающая сторона отвечает за закрытие файла.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:  w,
		bw: bufio.NewWriter(w),
	}
}

// Add добавляет пару key/value в SSTable.
// Ключи должны быть строго возрастающими: повторы и нарушение порядка возвращают ErrUnorderedKeys.
// Пустой ключ (nil или []byte{}) допустим, если он встречается только один раз и идёт первым.
func (w *Writer) Add(key, value []byte) error {
	if w.closed {
		return errors.New("sstable: writer closed")
	}

	// Инвариант: ключи строго возрастают. Без этого Reader не сможет делать бинпоиск по индексу.
	if w.prevKey != nil && bytes.Compare(key, w.prevKey) <= 0 {
		return fmt.Errorf("%w: prev=%q new=%q", ErrUnorderedKeys, w.prevKey, key)
	}

	// Если это первая запись блока — запоминаем её ключ для индекса.
	if w.curBlock.Len() == 0 {
		w.curBlockKey0 = append(w.curBlockKey0[:0], key...)
	}

	// Кодируем запись: [key_len uvarint][key][val_len uvarint][val] в буфер блока.
	// Используем небольшой стек-буфер под varint, чтобы не аллоцировать на каждой записи.
	var lenBuf [binary.MaxVarintLen64]byte

	n := binary.PutUvarint(lenBuf[:], uint64(len(key)))
	w.curBlock.Write(lenBuf[:n])
	w.curBlock.Write(key)

	n = binary.PutUvarint(lenBuf[:], uint64(len(value)))
	w.curBlock.Write(lenBuf[:n])
	w.curBlock.Write(value)

	// Обновляем prevKey (защитной копией — key могут изменить снаружи).
	w.prevKey = append(w.prevKey[:0], key...)

	// Если блок дорос до целевого размера — закрываем его.
	if w.curBlock.Len() >= targetBlockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	return nil
}

// flushBlock записывает текущий буфер блока в выходной поток и фиксирует запись в индексе.
// Безопасен для пустого блока (no-op).
func (w *Writer) flushBlock() error {
	if w.curBlock.Len() == 0 {
		return nil
	}

	offset := w.written
	length := uint32(w.curBlock.Len())

	if _, err := w.bw.Write(w.curBlock.Bytes()); err != nil {
		return fmt.Errorf("sstable: write data block: %w", err)
	}
	w.written += uint64(length)

	// Фиксируем индекс: для firstKey делаем независимую копию,
	// потому что curBlockKey0 будет переиспользоваться следующим блоком.
	w.index = append(w.index, indexEntry{
		firstKey: append([]byte(nil), w.curBlockKey0...),
		offset:   offset,
		length:   length,
	})

	w.curBlock.Reset()
	w.curBlockKey0 = w.curBlockKey0[:0]
	return nil
}

// Close завершает запись: дофлашивает последний блок, пишет index block и footer.
// После Close писать в Writer нельзя.
func (w *Writer) Close() error {
	if w.closed {
		return nil // идемпотентность Close — допустимо в Go
	}
	w.closed = true

	// 1. Закрыть последний (возможно, неполный) блок.
	if err := w.flushBlock(); err != nil {
		return err
	}

	// 2. Записать index block.
	indexOffset := w.written
	var lenBuf [binary.MaxVarintLen64]byte
	for _, e := range w.index {
		// [first_key_len uvarint][first_key][offset uint64 BE][length uint32 BE]
		n := binary.PutUvarint(lenBuf[:], uint64(len(e.firstKey)))
		if _, err := w.bw.Write(lenBuf[:n]); err != nil {
			return fmt.Errorf("sstable: write index entry: %w", err)
		}
		w.written += uint64(n)

		if _, err := w.bw.Write(e.firstKey); err != nil {
			return fmt.Errorf("sstable: write index entry: %w", err)
		}
		w.written += uint64(len(e.firstKey))

		var off [8]byte
		binary.BigEndian.PutUint64(off[:], e.offset)
		if _, err := w.bw.Write(off[:]); err != nil {
			return fmt.Errorf("sstable: write index entry: %w", err)
		}
		w.written += 8

		var ln [4]byte
		binary.BigEndian.PutUint32(ln[:], e.length)
		if _, err := w.bw.Write(ln[:]); err != nil {
			return fmt.Errorf("sstable: write index entry: %w", err)
		}
		w.written += 4
	}
	indexLength := uint32(w.written - indexOffset)

	// 3. Записать footer (16 байт фиксированной длины).
	var footer [footerSize]byte
	binary.BigEndian.PutUint64(footer[0:8], indexOffset)
	binary.BigEndian.PutUint32(footer[8:12], indexLength)
	binary.BigEndian.PutUint32(footer[12:16], magicNumber)
	if _, err := w.bw.Write(footer[:]); err != nil {
		return fmt.Errorf("sstable: write footer: %w", err)
	}

	// 4. Финальный flush буфера в нижележащий io.Writer.
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("sstable: flush: %w", err)
	}
	return nil
}

// --- Reader ---

// Reader читает SSTable. Использует io.ReaderAt — random access чтения,
// подходит для *os.File и для bytes.NewReader (тестируемость).
//
// При создании Reader сразу читает footer и индекс блоков с диска и держит
// индекс в памяти. Это разовая стоимость на открытие, но дальше каждый
// Iterator() работает без обращения к индексу на диске.
type Reader struct {
	r     io.ReaderAt
	size  int64
	index []indexEntry // индекс целиком в памяти
}

// NewReader открывает SSTable, читает и валидирует footer, загружает индекс.
// size — полный размер файла; нужен, чтобы знать, откуда читать footer.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	if size < footerSize {
		return nil, fmt.Errorf("%w: size=%d < footerSize=%d", ErrCorruptFooter, size, footerSize)
	}

	// 1. Прочитать footer (последние footerSize байт).
	var footer [footerSize]byte
	if _, err := r.ReadAt(footer[:], size-footerSize); err != nil {
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	indexOffset := binary.BigEndian.Uint64(footer[0:8])
	indexLength := binary.BigEndian.Uint32(footer[8:12])
	magic := binary.BigEndian.Uint32(footer[12:16])

	if magic != magicNumber {
		return nil, fmt.Errorf("%w: magic=%#x", ErrCorruptFooter, magic)
	}
	// Sanity: индекс должен лежать строго перед footer.
	if uint64(size) < uint64(footerSize)+uint64(indexLength) ||
		indexOffset+uint64(indexLength)+uint64(footerSize) != uint64(size) {
		return nil, fmt.Errorf("%w: indexOffset=%d indexLength=%d size=%d",
			ErrCorruptFooter, indexOffset, indexLength, size)
	}

	// 2. Прочитать index block целиком.
	indexBuf := make([]byte, indexLength)
	if _, err := r.ReadAt(indexBuf, int64(indexOffset)); err != nil {
		return nil, fmt.Errorf("sstable: read index: %w", err)
	}

	// 3. Распарсить индекс.
	index, err := parseIndex(indexBuf)
	if err != nil {
		return nil, err
	}

	return &Reader{r: r, size: size, index: index}, nil
}

// parseIndex распарсивает байты Index Block в []indexEntry.
func parseIndex(buf []byte) ([]indexEntry, error) {
	var index []indexEntry
	for pos := 0; pos < len(buf); {
		klen, n := binary.Uvarint(buf[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("%w: bad uvarint key_len at pos=%d", ErrCorruptIndex, pos)
		}
		pos += n
		if pos+int(klen)+8+4 > len(buf) {
			return nil, fmt.Errorf("%w: truncated entry at pos=%d", ErrCorruptIndex, pos-n)
		}
		// Защитная копия firstKey: индекс будет жить долго, чужой буфер мы не держим.
		firstKey := append([]byte(nil), buf[pos:pos+int(klen)]...)
		pos += int(klen)
		offset := binary.BigEndian.Uint64(buf[pos : pos+8])
		pos += 8
		length := binary.BigEndian.Uint32(buf[pos : pos+4])
		pos += 4
		index = append(index, indexEntry{firstKey: firstKey, offset: offset, length: length})
	}
	return index, nil
}

// findStartBlock возвращает индекс блока, в котором может находиться start.
// Если start == nil, начинаем с самого первого блока.
//
// Возвращаемое значение -1 означает, что start заведомо больше всех ключей в файле.
func (r *Reader) findStartBlock(start []byte) int {
	if len(r.index) == 0 {
		return -1
	}
	if start == nil {
		return 0
	}
	// Найти последний блок, у которого firstKey <= start.
	// Бинпоиск: ищем первый блок с firstKey > start, отступаем на 1.
	lo, hi := 0, len(r.index)
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(r.index[mid].firstKey, start) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	// lo — первый блок с firstKey > start. Нужный блок — на 1 левее.
	if lo == 0 {
		// Все firstKey > start, нужный блок — самый первый
		// (start может быть меньше всех ключей файла, но возможно лежит в первом блоке).
		return 0
	}
	return lo - 1
}

// Iterator возвращает итератор по диапазону [start, end).
// start == nil — с начала файла, end == nil — до конца файла.
func (r *Reader) Iterator(start, end []byte) (*Iter, error) {
	// Проверка пустого диапазона.
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return &Iter{}, nil // пустой итератор
	}

	startBlock := r.findStartBlock(start)
	if startBlock < 0 {
		return &Iter{}, nil // нет блоков
	}

	it := &Iter{
		r:        r,
		end:      end,
		blockIdx: startBlock,
	}

	// Загрузить первый блок и спозиционироваться.
	if err := it.loadBlock(); err != nil {
		return nil, err
	}
	if start != nil {
		it.seekInBlock(start)
	}
	return it, nil
}

// --- Iter ---

// Iter — ленивый итератор по парам SSTable.
// Загружает блоки по одному, не держит весь файл в памяти.
type Iter struct {
	r        *Reader
	end      []byte // верхняя граница (исключительно); nil = +∞
	blockIdx int    // индекс текущего блока в r.index
	block    []byte // буфер текущего блока
	pos      int    // позиция чтения в block

	closed bool
}

// loadBlock читает с диска блок r.index[blockIdx] в it.block.
func (it *Iter) loadBlock() error {
	if it.blockIdx >= len(it.r.index) {
		it.block = nil
		return nil
	}
	e := it.r.index[it.blockIdx]
	if cap(it.block) < int(e.length) {
		it.block = make([]byte, e.length)
	} else {
		it.block = it.block[:e.length]
	}
	if _, err := it.r.r.ReadAt(it.block, int64(e.offset)); err != nil {
		return fmt.Errorf("sstable: read block %d: %w", it.blockIdx, err)
	}
	it.pos = 0
	return nil
}

// seekInBlock проматывает it.block до первой записи с key >= target.
// Если такой записи нет в блоке — pos станет равным длине блока.
func (it *Iter) seekInBlock(target []byte) {
	for it.pos < len(it.block) {
		savedPos := it.pos
		key, _, ok := it.peekRecord()
		if !ok {
			return // повреждение или конец, оставляем как есть
		}
		if bytes.Compare(key, target) >= 0 {
			it.pos = savedPos
			return
		}
		it.advanceRecord()
	}
}

// peekRecord парсит запись в it.block[it.pos] не сдвигая pos.
// Возвращает (key, value, true) при успехе или (_, _, false) при повреждении/конце.
func (it *Iter) peekRecord() (key, value []byte, ok bool) {
	if it.pos >= len(it.block) {
		return nil, nil, false
	}
	klen, n := binary.Uvarint(it.block[it.pos:])
	if n <= 0 {
		return nil, nil, false
	}
	keyStart := it.pos + n
	keyEnd := keyStart + int(klen)
	if keyEnd > len(it.block) {
		return nil, nil, false
	}
	vlen, m := binary.Uvarint(it.block[keyEnd:])
	if m <= 0 {
		return nil, nil, false
	}
	valStart := keyEnd + m
	valEnd := valStart + int(vlen)
	if valEnd > len(it.block) {
		return nil, nil, false
	}
	return it.block[keyStart:keyEnd], it.block[valStart:valEnd], true
}

// advanceRecord двигает pos за конец текущей записи (нужно после peekRecord).
func (it *Iter) advanceRecord() {
	klen, n := binary.Uvarint(it.block[it.pos:])
	keyEnd := it.pos + n + int(klen)
	vlen, m := binary.Uvarint(it.block[keyEnd:])
	valEnd := keyEnd + m + int(vlen)
	it.pos = valEnd
}

// Next возвращает следующую пару из итератора.
// Возвращаемые key/value — это копии, безопасно держать после следующего Next.
func (it *Iter) Next() (key, value []byte, ok bool, err error) {
	if it.closed {
		return nil, nil, false, ErrIteratorClosed
	}
	// Пустой итератор (вернули &Iter{} как «ничего нет»): r не инициализирован.
	if it.r == nil {
		return nil, nil, false, nil
	}
	for {
		// Если в текущем блоке кончились записи — переходим к следующему.
		if it.pos >= len(it.block) {
			it.blockIdx++
			if it.blockIdx >= len(it.r.index) {
				return nil, nil, false, nil // конец файла
			}
			if err := it.loadBlock(); err != nil {
				return nil, nil, false, err
			}
			continue
		}

		k, v, okRec := it.peekRecord()
		if !okRec {
			return nil, nil, false, fmt.Errorf("%w: at block=%d pos=%d",
				ErrCorruptBlock, it.blockIdx, it.pos)
		}

		// Проверка верхней границы.
		if it.end != nil && bytes.Compare(k, it.end) >= 0 {
			return nil, nil, false, nil
		}

		it.advanceRecord()

		// Защитные копии: блок может быть переиспользован при загрузке следующего.
		keyCopy := append([]byte(nil), k...)
		valCopy := append([]byte(nil), v...)
		return keyCopy, valCopy, true, nil
	}
}

// Close освобождает буфер блока и помечает итератор как закрытый.
func (it *Iter) Close() error {
	it.closed = true
	it.block = nil
	return nil
}
