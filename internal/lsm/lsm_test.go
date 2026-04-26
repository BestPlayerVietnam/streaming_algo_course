//go:build day2

package lsm

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestLSM_PutGetBasic(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	if err := e.Put(ctx, []byte("alice"), []byte("Wonderland")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := e.Get(ctx, []byte("alice"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "Wonderland" {
		t.Fatalf("Get: got=%q want=%q", got, "Wonderland")
	}
}

func TestLSM_GetMissingReturnsErrNotFound(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	_, err = e.Get(ctx, []byte("nope"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: ожидали ErrNotFound, получили %v", err)
	}
}

func TestLSM_PutOverwrite(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	if err := e.Put(ctx, []byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put1: %v", err)
	}
	if err := e.Put(ctx, []byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Put2: %v", err)
	}
	got, err := e.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("got=%q want=%q", got, "v2")
	}
}

func TestLSM_DeleteHidesKey(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	if err := e.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.Delete(ctx, []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = e.Get(ctx, []byte("k"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get после Delete: ожидали ErrNotFound, получили %v", err)
	}
}

func TestLSM_DeleteNonexistentIsOK(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	// На уровне LSM Delete несуществующего ключа — это валидная операция,
	// которая просто записывает tombstone. Не ошибка.
	if err := e.Delete(ctx, []byte("nope")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestLSM_PutEmptyValue(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	// Пустое значение — легитимный Put, не tombstone. Это разница между
	// "ключ есть, значение пустое" и "ключ удалён".
	if err := e.Put(ctx, []byte("flag"), []byte{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := e.Get(ctx, []byte("flag"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte{}) {
		t.Fatalf("got=%q want=пусто", got)
	}
}

func TestLSM_OperationsAfterClose(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := e.Put(ctx, []byte("k"), []byte("v")); !errors.Is(err, ErrClosed) {
		t.Errorf("Put after Close: ожидали ErrClosed, получили %v", err)
	}
	if _, err := e.Get(ctx, []byte("k")); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close: ожидали ErrClosed, получили %v", err)
	}
	// Повторный Close идемпотентен.
	if err := e.Close(); err != nil {
		t.Errorf("повторный Close: %v", err)
	}
}

// TestLSM_FlushOnThreshold проверяет, что после преодоления порога Memtable
// данные оказываются на диске в SSTable, и Get продолжает их находить.
func TestLSM_FlushOnThreshold(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	// Маленький порог, чтобы триггер сработал на 10 КБ значениях быстро.
	e, err := Open(Options{Dir: dir, MemtableFlushThreshold: 4 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	bigVal := bytes.Repeat([]byte("x"), 1024)
	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		if err := e.Put(ctx, key, bigVal); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
	}
	// На 20 КБ + порог 4 КБ должен был сработать как минимум один флаш.
	if len(e.sstables) == 0 {
		t.Fatal("ожидали хотя бы один SSTable после переполнения Memtable")
	}

	// Все ключи должны быть найдены — независимо от того, в Memtable они или в SSTable.
	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		got, err := e.Get(ctx, key)
		if err != nil {
			t.Errorf("Get %q: %v", key, err)
			continue
		}
		if !bytes.Equal(got, bigVal) {
			t.Errorf("Get %q: значение не совпало", key)
		}
	}
}

// TestLSM_FlushOnClose проверяет, что Close сбрасывает оставшуюся Memtable.
// После Close + Open данные должны быть видны (через SSTable).
func TestLSM_FlushOnClose(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := e.Put(ctx, []byte("persisted"), []byte("yes")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	e2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open2: %v", err)
	}
	defer e2.Close()

	got, err := e2.Get(ctx, []byte("persisted"))
	if err != nil {
		t.Fatalf("Get после реоткрытия: %v", err)
	}
	if string(got) != "yes" {
		t.Fatalf("got=%q want=%q", got, "yes")
	}
}

// TestLSM_TombstoneAcrossLevels: ключ есть в SSTable, потом удаляется в Memtable.
// Get должен вернуть ErrNotFound, не значение из SSTable.
func TestLSM_TombstoneAcrossLevels(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir, MemtableFlushThreshold: 4 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	bigVal := bytes.Repeat([]byte("x"), 1024)
	// 1. Кладём ключ.
	if err := e.Put(ctx, []byte("k"), []byte("original")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// 2. Заполняем Memtable до флаша — теперь "k" в SSTable.
	for i := 0; i < 10; i++ {
		if err := e.Put(ctx, []byte{byte('A' + i)}, bigVal); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
	}
	if len(e.sstables) == 0 {
		t.Fatal("ожидали флаш")
	}

	// 3. Удаляем "k" — tombstone в новой Memtable.
	if err := e.Delete(ctx, []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// 4. Get должен увидеть tombstone и вернуть NotFound, не "original".
	if _, err := e.Get(ctx, []byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get удалённого ключа: ожидали ErrNotFound, получили %v", err)
	}
}

// TestLSM_RecoveryFromWAL_NoFlush проверяет crash recovery без чистого Close:
// данные есть только в WAL (Memtable никогда не флашилась). При повторном Open
// они должны вернуться через replay.
func TestLSM_RecoveryFromWAL_NoFlush(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")

	e, err := Open(Options{Dir: dir, MemtableFlushThreshold: 1 << 30}) // огромный порог — никогда не флашить
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := e.Put(ctx, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put k1: %v", err)
	}
	if err := e.Put(ctx, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put k2: %v", err)
	}
	if err := e.Delete(ctx, []byte("k1")); err != nil {
		t.Fatalf("Delete k1: %v", err)
	}

	// Имитация kill -9: закрываем дескрипторы без флаша и без graceful shutdown.
	if err := e.crashClose(); err != nil {
		t.Fatalf("crashClose: %v", err)
	}

	// Повторный Open: WAL должен быть прочитан, Memtable восстановлена.
	e2, err := Open(Options{Dir: dir, MemtableFlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("Open2: %v", err)
	}
	defer e2.Close()

	// k1 удалён → ErrNotFound.
	if _, err := e2.Get(ctx, []byte("k1")); !errors.Is(err, ErrNotFound) {
		t.Errorf("k1 после recovery: ожидали ErrNotFound, получили %v", err)
	}
	// k2 — Put-запись, должна быть восстановлена.
	got, err := e2.Get(ctx, []byte("k2"))
	if err != nil {
		t.Fatalf("k2: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("k2: got=%q want=%q", got, "v2")
	}
}

func TestLSM_RecoveryFromWAL_AfterFlush(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")

	e, err := Open(Options{Dir: dir, MemtableFlushThreshold: 4 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Заполняем Memtable до флаша.
	bigVal := bytes.Repeat([]byte("x"), 1024)
	for i := 0; i < 10; i++ {
		if err := e.Put(ctx, []byte{byte('A' + i)}, bigVal); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if len(e.sstables) == 0 {
		t.Fatal("ожидали флаш")
	}

	// После флаша добавляем ещё пару записей — они только в новом WAL.
	if err := e.Put(ctx, []byte("after_flush"), []byte("post")); err != nil {
		t.Fatalf("Put after flush: %v", err)
	}

	// Имитация краша.
	if err := e.crashClose(); err != nil {
		t.Fatalf("crashClose: %v", err)
	}

	// Recovery.
	e2, err := Open(Options{Dir: dir, MemtableFlushThreshold: 4 * 1024})
	if err != nil {
		t.Fatalf("Open2: %v", err)
	}
	defer e2.Close()

	// Старая запись из SSTable.
	got, err := e2.Get(ctx, []byte("A"))
	if err != nil {
		t.Fatalf("Get A: %v", err)
	}
	if !bytes.Equal(got, bigVal) {
		t.Errorf("A: значение не совпадает")
	}

	// Запись из WAL после флаша.
	got, err = e2.Get(ctx, []byte("after_flush"))
	if err != nil {
		t.Fatalf("Get after_flush: %v", err)
	}
	if string(got) != "post" {
		t.Fatalf("after_flush: got=%q want=%q", got, "post")
	}
}

// TestLSM_CompactionRemovesDuplicates — Compaction Test из задания:
// записываем один и тот же ключ много раз (с разными значениями),
// после compaction в результирующем SSTable должна остаться одна запись
// с самым свежим значением.
func TestLSM_CompactionRemovesDuplicates(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	// Маленькие пороги: каждое значение будет создавать отдельный SSTable.
	e, err := Open(Options{
		Dir:                    dir,
		MemtableFlushThreshold: 1,   // флашить после каждого Put
		CompactionThreshold:    100, // сами вызовем в конце
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	// Один и тот же ключ "k" с 10 разными значениями.
	for i := 0; i < 10; i++ {
		val := []byte{byte('0' + i)}
		if err := e.Put(ctx, []byte("k"), val); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
	}

	// Get до compaction должен вернуть последнее значение ("9").
	got, err := e.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get до compaction: %v", err)
	}
	if string(got) != "9" {
		t.Fatalf("до compaction: got=%q want=%q", got, "9")
	}

	// Должно быть много SSTable.
	if len(e.sstables) < 2 {
		t.Fatalf("ожидали несколько SSTable до compaction, получили %d", len(e.sstables))
	}
	beforeCount := len(e.sstables)
	t.Logf("до compaction: %d SSTable", beforeCount)

	// Принудительно вызываем compaction (минуя порог).
	e.mu.Lock()
	if err := e.compactAllLocked(); err != nil {
		e.mu.Unlock()
		t.Fatalf("compactAllLocked: %v", err)
	}
	e.mu.Unlock()

	// После compaction должен быть один SSTable.
	if len(e.sstables) != 1 {
		t.Fatalf("после compaction: ожидали 1 SSTable, получили %d", len(e.sstables))
	}

	// Get после compaction всё равно возвращает "9".
	got, err = e.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get после compaction: %v", err)
	}
	if string(got) != "9" {
		t.Fatalf("после compaction: got=%q want=%q", got, "9")
	}
}

// TestLSM_CompactionDropsTombstones проверяет, что в результате compaction
// tombstone и стёртые им записи физически удалены — итерация по новому
// SSTable не выдаст этот ключ.
func TestLSM_CompactionDropsTombstones(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{
		Dir:                    dir,
		MemtableFlushThreshold: 1,
		CompactionThreshold:    100,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	if err := e.Put(ctx, []byte("dead"), []byte("body")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.Put(ctx, []byte("alive"), []byte("hello")); err != nil {
		t.Fatalf("Put alive: %v", err)
	}
	if err := e.Delete(ctx, []byte("dead")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	e.mu.Lock()
	if err := e.compactAllLocked(); err != nil {
		e.mu.Unlock()
		t.Fatalf("compactAllLocked: %v", err)
	}
	e.mu.Unlock()

	// "dead" должен быть NotFound.
	if _, err := e.Get(ctx, []byte("dead")); !errors.Is(err, ErrNotFound) {
		t.Errorf("dead после compaction: ожидали ErrNotFound, получили %v", err)
	}
	// "alive" должен быть жив.
	got, err := e.Get(ctx, []byte("alive"))
	if err != nil {
		t.Fatalf("alive: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("alive: got=%q want=%q", got, "hello")
	}

	// В единственном SSTable должна быть только одна запись (alive).
	if len(e.sstables) != 1 {
		t.Fatalf("ожидали 1 SSTable, получили %d", len(e.sstables))
	}
	it, err := e.sstables[0].reader.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()
	count := 0
	for {
		_, _, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		count++
	}
	if count != 1 {
		t.Errorf("в SSTable записей: got=%d want=1", count)
	}
}

// TestLSM_CompactionAutoTrigger проверяет, что compaction срабатывает
// автоматически по достижении CompactionThreshold.
func TestLSM_CompactionAutoTrigger(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{
		Dir:                    dir,
		MemtableFlushThreshold: 1,
		CompactionThreshold:    3,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	// 10 разных ключей → 10 флашей → должен сработать compaction (порог 3).
	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		if err := e.Put(ctx, key, []byte{byte('A' + i)}); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
	}

	// При пороге 3 после каждого 3-го флаша compactAllLocked сольёт всё в 1.
	// То есть к концу должно остаться <= 3 файлов (последние 1–3 ещё не доросли до порога).
	if len(e.sstables) > 3 {
		t.Errorf("ожидали ≤ 3 SSTable, получили %d", len(e.sstables))
	}

	// Все ключи всё ещё доступны.
	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		want := []byte{byte('A' + i)}
		got, err := e.Get(ctx, key)
		if err != nil {
			t.Errorf("Get %q: %v", key, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get %q: got=%q want=%q", key, got, want)
		}
	}
}

// TestLSM_EmptyKey проверяет, что пустой ключ обрабатывается корректно.
// Это легитимный сценарий: ключи — байты произвольной длины, включая нулевой.
func TestLSM_EmptyKey(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	// Пустой ключ + непустое значение.
	if err := e.Put(ctx, []byte{}, []byte("empty_key_value")); err != nil {
		t.Fatalf("Put empty key: %v", err)
	}
	got, err := e.Get(ctx, []byte{})
	if err != nil {
		t.Fatalf("Get empty key: %v", err)
	}
	if string(got) != "empty_key_value" {
		t.Errorf("got=%q want=%q", got, "empty_key_value")
	}

	// Delete пустого ключа.
	if err := e.Delete(ctx, []byte{}); err != nil {
		t.Fatalf("Delete empty key: %v", err)
	}
	if _, err := e.Get(ctx, []byte{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("после Delete: ожидали ErrNotFound, получили %v", err)
	}
}

// TestLSM_NilVsEmptyValue проверяет, что nil и []byte{} как значения
// обрабатываются одинаково (как пустое Put-значение, не tombstone).
func TestLSM_NilVsEmptyValue(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	if err := e.Put(ctx, []byte("a"), nil); err != nil {
		t.Fatalf("Put nil: %v", err)
	}
	got, err := e.Get(ctx, []byte("a"))
	if err != nil {
		t.Fatalf("Get a: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("nil value: ожидали пусто, получили %q", got)
	}

	if err := e.Put(ctx, []byte("b"), []byte{}); err != nil {
		t.Fatalf("Put empty: %v", err)
	}
	got, err = e.Get(ctx, []byte("b"))
	if err != nil {
		t.Fatalf("Get b: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty value: ожидали пусто, получили %q", got)
	}
}

// TestLSM_LargeValue проверяет работу с большими значениями (1 MB),
// превышающими типичный размер блока SSTable.
func TestLSM_LargeValue(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir, MemtableFlushThreshold: 4 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	huge := bytes.Repeat([]byte("ABCDEFGH"), 128*1024) // 1 МБ
	if err := e.Put(ctx, []byte("big"), huge); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := e.Get(ctx, []byte("big"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, huge) {
		t.Fatalf("значение не совпадает (lengths: got=%d want=%d)", len(got), len(huge))
	}
}

// TestLSM_RepeatedPutsThenGet — массовое перезаписывание одного ключа,
// проверяет что побеждает последняя версия (через несколько слоёв SSTable).
func TestLSM_RepeatedPutsThenGet(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{
		Dir:                    dir,
		MemtableFlushThreshold: 256, // часто флашится → много SSTable
		CompactionThreshold:    100, // не сжимать
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	const N = 50
	for i := 0; i < N; i++ {
		val := []byte{byte(i % 256)}
		if err := e.Put(ctx, []byte("k"), val); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
	}

	got, err := e.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := []byte{byte((N - 1) % 256)}
	if !bytes.Equal(got, want) {
		t.Errorf("после %d Put: got=%v want=%v", N, got, want)
	}
}

// TestLSM_DeleteThenPut — поведение «удалили, потом снова положили».
// Tombstone должен быть «забит» новым Put, Get возвращает значение.
func TestLSM_DeleteThenPut(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Dir: dir, MemtableFlushThreshold: 4 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	// 1. Кладём, удаляем, ещё кладём — всё в Memtable.
	if err := e.Put(ctx, []byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put1: %v", err)
	}
	if err := e.Delete(ctx, []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := e.Put(ctx, []byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Put2: %v", err)
	}

	got, err := e.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("got=%q want=%q", got, "v2")
	}

	// 2. Принудительно флашим, проверяем, что после flush + recovery всё ещё ок.
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	e2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open2: %v", err)
	}
	defer e2.Close()

	got, err = e2.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get после восстановления: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("после восстановления: got=%q want=%q", got, "v2")
	}
}

// TestLSM_ManyKeysFullCycle — большой нагрузочный тест:
// много ключей, флаши, compaction, всё ещё доступно.
func TestLSM_ManyKeysFullCycle(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{
		Dir:                    dir,
		MemtableFlushThreshold: 8 * 1024,
		CompactionThreshold:    3,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	const N = 1000
	val := bytes.Repeat([]byte("x"), 64)
	for i := 0; i < N; i++ {
		key := []byte(formatKey6(i))
		if err := e.Put(ctx, key, val); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
	}

	// Все ключи должны быть найдены.
	for i := 0; i < N; i++ {
		key := []byte(formatKey6(i))
		got, err := e.Get(ctx, key)
		if err != nil {
			t.Errorf("Get %s: %v", key, err)
			continue
		}
		if !bytes.Equal(got, val) {
			t.Errorf("Get %s: значение не совпадает", key)
		}
	}

	// Промежуточные миссы.
	if _, err := e.Get(ctx, []byte("nokey")); !errors.Is(err, ErrNotFound) {
		t.Errorf("ожидали ErrNotFound для несуществующего, получили %v", err)
	}
}

// formatKey6 формирует 6-значный ключ "k_NNNNN" с лексикографическим порядком.
func formatKey6(i int) string {
	const digits = "0123456789"
	buf := []byte("k_00000")
	for pos := 6; pos >= 2; pos-- {
		buf[pos] = digits[i%10]
		i /= 10
	}
	return string(buf)
}
