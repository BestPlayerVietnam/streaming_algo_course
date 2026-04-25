package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Формат записи WAL на диске:
//
//   [crc32: 4 байта BE][type: 1 байт][key_len: uvarint][key][val_len: uvarint][value]
//
// CRC32 (IEEE) считается по конкатенации: type | key_len | key | val_len | value.
// Это позволяет отличить корректно записанную запись от обрыва (torn write):
// если процесс упал в середине записи, на чтении CRC не сойдётся —
// мы трактуем это как естественный конец лога и тихо останавливаемся.
//
// При OpDelete value игнорируется при записи и при чтении (всегда пустой).

// Ошибки пакета.
var (
	ErrNotImplemented = errors.New("wal: функция не реализована")
	ErrUnknownOp      = errors.New("wal: неизвестный тип операции")
)

// OpType — тип операции в WAL (Put или Delete).
type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// Record — запись в логе.
// Для OpDelete поле Value не несёт смысла (всегда nil/пусто).
type Record struct {
	Type  OpType
	Key   []byte
	Value []byte
}

// crcTable — таблица для CRC32 IEEE. crc32.ChecksumIEEE использует её внутри,
// но явное создание полезно, если позже захотим переиспользовать crc32.Hash32.
var crcTable = crc32.MakeTable(crc32.IEEE)

// Writer пишет append-only лог в io.Writer.
// Не закрывает нижележащий writer; вызывающая сторона отвечает за файл.
//
// ВАЖНО: для надёжности при crash recovery в LSM Engine после каждого Append
// нужно вызывать Sync() (если writer — *os.File). Этот пакет не делает sync
// автоматически, чтобы оставить решение о частоте sync'ов на уровень выше.
type Writer struct {
	bw     *bufio.Writer
	closed bool
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

// Append добавляет одну запись в лог.
// Не делает sync — это ответственность вызывающей стороны.
func (w *Writer) Append(rec Record) error {
	if w.closed {
		return errors.New("wal: writer closed")
	}
	if rec.Type != OpPut && rec.Type != OpDelete {
		return fmt.Errorf("%w: %d", ErrUnknownOp, rec.Type)
	}

	// Сборка payload (то, по чему считается CRC) в один буфер: type | key_len | key | val_len | value.
	// Заранее посчитать максимальный размер и взять один make — экономит аллокации.
	var lenBuf [binary.MaxVarintLen64]byte

	// Считаем длину payload.
	keyLenBytes := binary.PutUvarint(lenBuf[:], uint64(len(rec.Key)))
	keyLenHeader := append([]byte(nil), lenBuf[:keyLenBytes]...) // сохранить, lenBuf переиспользуется

	valLenBytes := binary.PutUvarint(lenBuf[:], uint64(len(rec.Value)))
	valLenHeader := append([]byte(nil), lenBuf[:valLenBytes]...)

	payloadLen := 1 + len(keyLenHeader) + len(rec.Key) + len(valLenHeader) + len(rec.Value)
	payload := make([]byte, 0, payloadLen)
	payload = append(payload, byte(rec.Type))
	payload = append(payload, keyLenHeader...)
	payload = append(payload, rec.Key...)
	payload = append(payload, valLenHeader...)
	payload = append(payload, rec.Value...)

	crc := crc32.Checksum(payload, crcTable)

	// Пишем: [crc32 BE][payload].
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc)
	if _, err := w.bw.Write(crcBuf[:]); err != nil {
		return fmt.Errorf("wal: write crc: %w", err)
	}
	if _, err := w.bw.Write(payload); err != nil {
		return fmt.Errorf("wal: write payload: %w", err)
	}
	// Принудительно сбрасываем буфер bufio в нижележащий writer на каждом Append.
	// Иначе данные могут зависнуть в буфере памяти и не попасть даже в страничный кеш ОС
	// до Close — это критично для надёжности WAL.
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	return nil
}

// Close завершает запись, флашит буфер.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.bw.Flush()
}

// --- Reader ---

// Reader последовательно читает WAL.
// Использование: NewReader -> много раз Next, пока ok=false.
//
// Семантика конца лога:
//   - чистый EOF между записями → (zero, false, nil)
//   - EOF посреди записи → (zero, false, nil) — лог оборван (вероятно, kill -9)
//   - CRC mismatch → (zero, false, nil) — то же самое, обрыв в момент записи
//
// В обоих случаях ошибка не возвращается: это нормальный конец восстановления.
//
// Реальная ошибка (например, повреждённое значение типа операции) возвращается явно.
type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReader(r)}
}

// Next читает следующую запись.
// (rec, true, nil)  — успешно прочитанная запись.
// (rec, false, nil) — конец лога (естественный или обрыв при crash).
// (rec, false, err) — настоящая ошибка чтения, ниже Reader использовать нельзя.
func (r *Reader) Next() (Record, bool, error) {
	// 1. Прочитать CRC (4 байта).
	var crcBuf [4]byte
	n, err := io.ReadFull(r.br, crcBuf[:])
	if err == io.EOF {
		// Чистый конец лога — между записями.
		return Record{}, false, nil
	}
	if err == io.ErrUnexpectedEOF {
		// Лог оборван внутри CRC — трактуем как конец, не как ошибку.
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("wal: read crc: %w", err)
	}
	_ = n
	expectedCRC := binary.BigEndian.Uint32(crcBuf[:])

	// 2. Прочитать type (1 байт).
	typeByte, err := r.br.ReadByte()
	if err != nil {
		// Внутри записи EOF — обрыв, считаем концом.
		return Record{}, false, nil
	}

	// 3. Прочитать key_len (uvarint).
	keyLen, err := binary.ReadUvarint(r.br)
	if err != nil {
		return Record{}, false, nil
	}

	// 4. Прочитать key.
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r.br, key); err != nil {
		return Record{}, false, nil
	}

	// 5. Прочитать val_len (uvarint).
	valLen, err := binary.ReadUvarint(r.br)
	if err != nil {
		return Record{}, false, nil
	}

	// 6. Прочитать value.
	value := make([]byte, valLen)
	if _, err := io.ReadFull(r.br, value); err != nil {
		return Record{}, false, nil
	}

	// 7. Пересобрать payload и проверить CRC.
	// Дублирование сборки — цена честной проверки, оверхед только на чтении (редкая операция).
	var lenBuf [binary.MaxVarintLen64]byte
	klBytes := binary.PutUvarint(lenBuf[:], keyLen)
	klHeader := append([]byte(nil), lenBuf[:klBytes]...)
	vlBytes := binary.PutUvarint(lenBuf[:], valLen)
	vlHeader := append([]byte(nil), lenBuf[:vlBytes]...)

	payload := make([]byte, 0, 1+len(klHeader)+len(key)+len(vlHeader)+len(value))
	payload = append(payload, typeByte)
	payload = append(payload, klHeader...)
	payload = append(payload, key...)
	payload = append(payload, vlHeader...)
	payload = append(payload, value...)

	if crc32.Checksum(payload, crcTable) != expectedCRC {
		// CRC не сошёлся: torn write, граница оборванного лога.
		return Record{}, false, nil
	}

	op := OpType(typeByte)
	if op != OpPut && op != OpDelete {
		// Тип валидной записи — не Put/Delete: это уже не «обрыв», а реальное повреждение.
		return Record{}, false, fmt.Errorf("%w: %d", ErrUnknownOp, typeByte)
	}

	return Record{Type: op, Key: key, Value: value}, true, nil
}
