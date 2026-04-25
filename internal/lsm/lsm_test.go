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
