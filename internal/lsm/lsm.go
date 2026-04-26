package lsm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"kvschool/internal/skiplist"
	"kvschool/internal/sstable"
	"kvschool/internal/wal"
)

// Ошибки пакета.
var (
	ErrNotFound = errors.New("lsm: ключ не найден")
	ErrClosed   = errors.New("lsm: engine закрыт")
)

// Внутренний формат значения в Memtable и SSTable:
//
//	[tag: 1 байт][user_value: N байт]
//
// Тег нужен, чтобы отличить Put с пустым значением (легитимный) от Delete (tombstone).
// Кодирование инкапсулировано в encodeValue / decodeValue —
// другие части кода должны использовать только эти функции, не лазить в байты напрямую.
const (
	tagPut    byte = 0x00
	tagDelete byte = 0x01
)

// encodeValue превращает (op, user_value) во внутреннее представление для хранения.
// Для Delete user_value игнорируется (всегда сохраняется только тег).
func encodeValue(op byte, val []byte) []byte {
	if op == tagDelete {
		return []byte{tagDelete}
	}
	out := make([]byte, 1+len(val))
	out[0] = tagPut
	copy(out[1:], val)
	return out
}

// decodeValue разбирает внутреннее представление.
// (val, true) — Put-запись, val — пользовательское значение (может быть пустым срезом).
// (nil, false) — tombstone (запись удалена).
func decodeValue(internal []byte) (val []byte, isPut bool) {
	if len(internal) == 0 {
		// Защита от мусора: пустой internal трактуем как tombstone.
		return nil, false
	}
	switch internal[0] {
	case tagPut:
		return internal[1:], true
	case tagDelete:
		return nil, false
	default:
		// Неизвестный тег: trifle defensive — относимся как к tombstone,
		// чтобы не отдать наружу мусор.
		return nil, false
	}
}

// Options задаёт параметры LSM движка.
type Options struct {
	Dir string // Директория для хранения WAL и SSTables (создаётся, если её нет).

	// Максимальный размер Memtable перед сбросом на диск (Flush).
	// 0 → значение по умолчанию (1 MiB).
	MemtableFlushThreshold int
}

// defaultMemtableThreshold — 1 MiB, как в задании.
const defaultMemtableThreshold = 1 * 1024 * 1024

// Engine — LSM движок (Memtable + WAL; на этом шаге без SSTable, без compaction).
//
// Конкурентность: все публичные методы защищены mu.
// Get использует RLock, Put/Delete/Close — Lock.
type Engine struct {
	opts Options

	mu  sync.RWMutex
	mem *skiplist.SkipList // активная Memtable

	wal     *wal.Writer
	walFile *os.File
	walPath string
	walSeq  uint64 // sequence number текущего WAL

	sstables []*sstableHandle // от старого (меньший seq) к свежему (больший seq)
	nextSeq  uint64           // следующий sequence number для нового SSTable

	closed bool
}

// Open открывает или создаёт LSM Engine в указанной директории.
// На этом шаге: создаёт директорию, открывает свежий WAL, создаёт пустую Memtable.
// Recovery из существующих WAL/SSTable будет добавлен на следующем шаге.
func Open(opts Options) (*Engine, error) {
	if opts.Dir == "" {
		return nil, errors.New("lsm: Options.Dir обязателен")
	}
	if opts.MemtableFlushThreshold <= 0 {
		opts.MemtableFlushThreshold = defaultMemtableThreshold
	}

	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("lsm: mkdir %s: %w", opts.Dir, err)
	}

	if err := cleanupTmp(opts.Dir); err != nil {
		return nil, fmt.Errorf("lsm: cleanup tmp: %w", err)
	}

	// 1. Открыть существующие SSTable.
	ssts, maxSstSeq, err := openExistingSSTables(opts.Dir)
	if err != nil {
		return nil, err
	}

	// 2. Восстановить Memtable из всех существующих WAL.
	mem, oldWALPaths, maxWalSeq, err := recoverFromWALs(opts.Dir)
	if err != nil {
		closeAllSSTables(ssts)
		return nil, fmt.Errorf("lsm: wal recovery: %w", err)
	}

	// 3. Выбрать sequence для нового WAL — больше всех существующих файлов.
	maxSeq := maxSstSeq
	if maxWalSeq > maxSeq {
		maxSeq = maxWalSeq
	}
	walSeq := maxSeq + 1
	walPath := filepath.Join(opts.Dir, walFileName(walSeq))
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		closeAllSSTables(ssts)
		return nil, fmt.Errorf("lsm: open wal: %w", err)
	}

	// 4. Удалить старые WAL только после успешного открытия нового.
	//    Если упадём здесь — в следующий Open старые WAL отработают повторно (идемпотентно).
	for _, p := range oldWALPaths {
		_ = os.Remove(p)
	}

	e := &Engine{
		opts:     opts,
		mem:      mem,
		wal:      wal.NewWriter(f),
		walFile:  f,
		walPath:  walPath,
		walSeq:   walSeq,
		sstables: ssts,
		nextSeq:  walSeq + 1,
	}
	return e, nil
}

func walFileName(seq uint64) string {
	return fmt.Sprintf("wal-%06d.log", seq)
}

func sstFileName(seq uint64) string {
	return fmt.Sprintf("sst-%06d.sst", seq)
}

// parseSeqFromName извлекает seq из имени вида "prefix-NNNNNN.ext".
func parseSeqFromName(name, prefix, ext string) (uint64, bool) {
	if len(name) < len(prefix)+len(ext)+1 {
		return 0, false
	}
	if name[:len(prefix)] != prefix || name[len(name)-len(ext):] != ext {
		return 0, false
	}
	seqStr := name[len(prefix) : len(name)-len(ext)]
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// cleanupTmp удаляет недописанные SSTable из предыдущего падения процесса.
func cleanupTmp(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".tmp" {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

// openExistingSSTables ищет файлы sst-*.sst в директории, открывает их,
// возвращает в порядке возрастания seq и максимальный seq.
func openExistingSSTables(dir string) ([]*sstableHandle, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, fmt.Errorf("lsm: readdir: %w", err)
	}
	type pending struct {
		seq  uint64
		path string
	}
	var pendings []pending
	var maxSeq uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		seq, ok := parseSeqFromName(e.Name(), "sst-", ".sst")
		if !ok {
			continue
		}
		pendings = append(pendings, pending{seq: seq, path: filepath.Join(dir, e.Name())})
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	// Также учитываем существующие WAL: новый seq должен быть больше них тоже.
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		seq, ok := parseSeqFromName(e.Name(), "wal-", ".log")
		if !ok {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	sort.Slice(pendings, func(i, j int) bool { return pendings[i].seq < pendings[j].seq })

	handles := make([]*sstableHandle, 0, len(pendings))
	for _, p := range pendings {
		f, err := os.Open(p.path)
		if err != nil {
			closeAllSSTables(handles)
			return nil, 0, fmt.Errorf("lsm: open sst %s: %w", p.path, err)
		}
		stat, err := f.Stat()
		if err != nil {
			f.Close()
			closeAllSSTables(handles)
			return nil, 0, fmt.Errorf("lsm: stat sst %s: %w", p.path, err)
		}
		r, err := sstable.NewReader(f, stat.Size())
		if err != nil {
			f.Close()
			closeAllSSTables(handles)
			return nil, 0, fmt.Errorf("lsm: open sst reader %s: %w", p.path, err)
		}
		handles = append(handles, &sstableHandle{seq: p.seq, path: p.path, file: f, reader: r})
	}
	return handles, maxSeq, nil
}

// recoverFromWALs читает все WAL-файлы в директории по возрастанию seq,
// накатывает их записи в новую Memtable и возвращает её, список прочитанных
// путей (для последующего удаления) и максимальный seq.
func recoverFromWALs(dir string) (*skiplist.SkipList, []string, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("readdir: %w", err)
	}

	type pending struct {
		seq  uint64
		path string
	}
	var pendings []pending
	var maxSeq uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		seq, ok := parseSeqFromName(e.Name(), "wal-", ".log")
		if !ok {
			continue
		}
		pendings = append(pendings, pending{seq: seq, path: filepath.Join(dir, e.Name())})
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	sort.Slice(pendings, func(i, j int) bool { return pendings[i].seq < pendings[j].seq })

	mem := skiplist.New(0)
	paths := make([]string, 0, len(pendings))
	for _, p := range pendings {
		if err := replayWAL(mem, p.path); err != nil {
			return nil, nil, 0, fmt.Errorf("replay %s: %w", p.path, err)
		}
		paths = append(paths, p.path)
	}
	return mem, paths, maxSeq, nil
}

// replayWAL читает один WAL-файл и применяет записи к Memtable.
// Использует штатный wal.Reader: torn writes на хвосте трактуются как конец лога.
func replayWAL(mem *skiplist.SkipList, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := wal.NewReader(f)
	for {
		rec, ok, err := r.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		switch rec.Type {
		case wal.OpPut:
			if err := mem.Put(rec.Key, encodeValue(tagPut, rec.Value)); err != nil {
				return fmt.Errorf("memtable put: %w", err)
			}
		case wal.OpDelete:
			if err := mem.Put(rec.Key, encodeValue(tagDelete, nil)); err != nil {
				return fmt.Errorf("memtable put: %w", err)
			}
		default:
			return fmt.Errorf("unknown op: %d", rec.Type)
		}
	}
}

func closeAllSSTables(hs []*sstableHandle) {
	for _, h := range hs {
		_ = h.Close()
	}
}

// Put добавляет/обновляет пару (key, value).
// Сначала запись в WAL (с fsync), затем в Memtable.
// Если WAL-запись не удалась — Memtable не модифицируется.
func (e *Engine) Put(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}

	if err := e.wal.Append(wal.Record{Type: wal.OpPut, Key: key, Value: value}); err != nil {
		return fmt.Errorf("lsm: wal append: %w", err)
	}
	if err := e.walFile.Sync(); err != nil {
		return fmt.Errorf("lsm: wal sync: %w", err)
	}

	if err := e.mem.Put(key, encodeValue(tagPut, value)); err != nil {
		return fmt.Errorf("lsm: memtable put: %w", err)
	}

	return e.maybeFlushLocked()
}

// Delete помечает ключ как удалённый (tombstone).
// На уровне LSM Delete всегда успешен — ключ может физически отсутствовать
// в Memtable, но при этом лежать в SSTable. Tombstone «забьёт» его при чтении.
func (e *Engine) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}

	if err := e.wal.Append(wal.Record{Type: wal.OpDelete, Key: key, Value: nil}); err != nil {
		return fmt.Errorf("lsm: wal append: %w", err)
	}
	if err := e.walFile.Sync(); err != nil {
		return fmt.Errorf("lsm: wal sync: %w", err)
	}

	if err := e.mem.Put(key, encodeValue(tagDelete, nil)); err != nil {
		return fmt.Errorf("lsm: memtable put: %w", err)
	}

	return e.maybeFlushLocked()
}

// Get ищет ключ в Memtable.
// На этом шаге SSTable не задействованы; будут добавлены позже.
func (e *Engine) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, ErrClosed
	}

	// 1. Memtable.
	if internal, err := e.mem.Get(key); err == nil {
		val, isPut := decodeValue(internal)
		if !isPut {
			return nil, ErrNotFound // tombstone
		}
		out := make([]byte, len(val))
		copy(out, val)
		return out, nil
	}

	// 2. SSTable от свежего к старому. Sequence больше = свежее.
	for i := len(e.sstables) - 1; i >= 0; i-- {
		h := e.sstables[i]
		val, found, isPut, err := lookupInSSTable(h.reader, key)
		if err != nil {
			return nil, fmt.Errorf("lsm: lookup in sst %d: %w", h.seq, err)
		}
		if !found {
			continue
		}
		if !isPut {
			return nil, ErrNotFound // tombstone
		}
		out := make([]byte, len(val))
		copy(out, val)
		return out, nil
	}

	return nil, ErrNotFound
}

// maybeFlushLocked флашит Memtable в SSTable, если она переросла порог.
// Должна вызываться под e.mu.Lock().
func (e *Engine) maybeFlushLocked() error {
	if e.mem.BytesUsed() < e.opts.MemtableFlushThreshold {
		return nil
	}
	return e.flushLocked()
}

// flushLocked сбрасывает текущую Memtable в новый SSTable файл.
// Шаги:
//  1. Записать SSTable во временный файл .tmp + Sync.
//  2. Атомарный Rename во финальное имя.
//  3. Удалить старый WAL.
//  4. Открыть новый WAL.
//  5. Заменить Memtable на пустую, добавить SSTable в список.
//
// При падении на шагах 1–2 .tmp удалится при следующем Open (cleanupTmp).
// При падении после 2 и до 4 — старый WAL может остаться; recovery накатит его поверх SSTable.
// Это безопасно: Put идемпотентен, повторное применение тех же записей даст тот же результат.
func (e *Engine) flushLocked() error {
	// Если Memtable пустая — флашить нечего.
	if e.mem.BytesUsed() == 0 {
		return nil
	}

	seq := e.nextSeq
	e.nextSeq++

	finalPath := filepath.Join(e.opts.Dir, sstFileName(seq))
	tmpPath := finalPath + ".tmp"

	// 1. Записать SSTable во временный файл.
	if err := writeMemtableToSSTable(e.mem, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("lsm: write sst: %w", err)
	}

	// 2. Rename — атомарная операция на POSIX.
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("lsm: rename sst: %w", err)
	}

	// 3. Открыть свежий SSTable для чтения.
	f, err := os.Open(finalPath)
	if err != nil {
		return fmt.Errorf("lsm: reopen sst: %w", err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("lsm: stat sst: %w", err)
	}
	rd, err := sstable.NewReader(f, stat.Size())
	if err != nil {
		f.Close()
		return fmt.Errorf("lsm: open sst reader: %w", err)
	}
	handle := &sstableHandle{seq: seq, path: finalPath, file: f, reader: rd}

	// 4. Закрыть и удалить старый WAL, открыть новый.
	if err := e.rotateWALLocked(); err != nil {
		// Если ротация WAL не удалась — handle новый SSTable всё равно валиден,
		// мы его добавим, но возвращаем ошибку наружу.
		e.sstables = append(e.sstables, handle)
		e.mem = skiplist.New(0)
		return fmt.Errorf("lsm: rotate wal: %w", err)
	}

	// 5. Зафиксировать новое состояние в памяти.
	e.sstables = append(e.sstables, handle)
	e.mem = skiplist.New(0)
	return nil
}

// rotateWALLocked: закрывает текущий WAL, удаляет его файл, открывает новый.
func (e *Engine) rotateWALLocked() error {
	if err := e.wal.Close(); err != nil {
		return fmt.Errorf("close old wal: %w", err)
	}
	if err := e.walFile.Close(); err != nil {
		return fmt.Errorf("close old wal file: %w", err)
	}
	if err := os.Remove(e.walPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old wal: %w", err)
	}

	newSeq := e.nextSeq
	e.nextSeq++
	newPath := filepath.Join(e.opts.Dir, walFileName(newSeq))
	f, err := os.OpenFile(newPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open new wal: %w", err)
	}
	e.wal = wal.NewWriter(f)
	e.walFile = f
	e.walPath = newPath
	e.walSeq = newSeq
	return nil
}

// writeMemtableToSSTable итерирует Memtable в порядке возрастания ключей
// и записывает её содержимое в SSTable файл по указанному пути.
func writeMemtableToSSTable(mem *skiplist.SkipList, path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer f.Close()

	w := sstable.NewWriter(f)
	it, err := mem.Scan(nil, nil)
	if err != nil {
		return fmt.Errorf("memtable scan: %w", err)
	}
	defer it.Close()

	for {
		k, v, ok, err := it.Next()
		if err != nil {
			return fmt.Errorf("memtable next: %w", err)
		}
		if !ok {
			break
		}
		// k и v — внутреннее представление (с тегом). Просто переносим как есть.
		if err := w.Add(k, v); err != nil {
			return fmt.Errorf("sst add: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("sst close: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	return nil
}

// lookupInSSTable ищет ровно один ключ в SSTable.
// Возвращает (val, found, isPut, err):
//
//	found=false   — ключа нет в этом файле, идём в следующий слой
//	found, isPut  — Put-запись, val — пользовательское значение
//	found, !isPut — tombstone, ключ "удалён" в этом файле
func lookupInSSTable(r *sstable.Reader, key []byte) ([]byte, bool, bool, error) {
	// Используем Iterator(key, key++): любой ключ строго больше key.
	// "key++" = key с добавленным нулевым байтом — лексикографически следующий.
	upper := make([]byte, len(key)+1)
	copy(upper, key)
	upper[len(key)] = 0x00

	it, err := r.Iterator(key, upper)
	if err != nil {
		return nil, false, false, err
	}
	defer it.Close()

	k, v, ok, err := it.Next()
	if err != nil {
		return nil, false, false, err
	}
	if !ok {
		return nil, false, false, nil
	}
	// Iterator(start, end) гарантирует k >= start; проверим точное равенство.
	if !bytesEqual(k, key) {
		return nil, false, false, nil
	}
	val, isPut := decodeValue(v)
	return val, true, isPut, nil
}

// bytesEqual — крошечная локальная замена bytes.Equal, чтобы не плодить импорты.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Close завершает работу: флашит и закрывает WAL.
// На следующем шаге также будет флашить Memtable в SSTable.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true

	var firstErr error

	// Флашим оставшуюся Memtable (если в ней что-то есть).
	// Это означает: после Close данные либо в SSTable, либо в WAL.
	if e.mem.BytesUsed() > 0 {
		if err := e.flushLocked(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("lsm: final flush: %w", err)
		}
	}

	if err := e.wal.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("lsm: wal close: %w", err)
	}
	if err := e.walFile.Sync(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("lsm: wal file sync: %w", err)
	}
	if err := e.walFile.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("lsm: wal file close: %w", err)
	}

	for _, h := range e.sstables {
		if err := h.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("lsm: sst close: %w", err)
		}
	}
	e.sstables = nil

	return firstErr
}

// crashClose имитирует «жёсткое» выключение: закрывает все file handles
// без флаша Memtable и без удаления старого WAL. Используется только в тестах
// для проверки crash recovery — в нормальной работе всегда вызывается Close.
//
// После crashClose Engine непригоден для дальнейшего использования.
func (e *Engine) crashClose() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true

	var firstErr error
	// Только закрываем файлы — никаких флашей и удалений.
	if err := e.walFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, h := range e.sstables {
		if err := h.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.sstables = nil
	return firstErr
}

type sstableHandle struct {
	seq    uint64
	path   string
	file   *os.File
	reader *sstable.Reader
}

func (h *sstableHandle) Close() error {
	if h.file == nil {
		return nil
	}
	err := h.file.Close()
	h.file = nil
	h.reader = nil
	return err
}
