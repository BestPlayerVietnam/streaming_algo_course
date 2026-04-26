package lsmstore

import (
	"context"
	"errors"

	"kvschool/internal/kv"
	"kvschool/internal/lsm"
)

// Store — реализация kv.Store поверх LSM движка.
// Всё «мясо» — в lsm.Engine; здесь только тонкая адаптация типов и ошибок.
type Store struct {
	engine *lsm.Engine
}

// Options — параметры стора. Сейчас только директория для файлов.
// Если в будущем понадобятся пороги — пробрасываются в lsm.Options.
type Options struct {
	Dir string
}

// Open открывает или создаёт LSM-стор в указанной директории.
func Open(opts Options) (*Store, error) {
	e, err := lsm.Open(lsm.Options{Dir: opts.Dir})
	if err != nil {
		return nil, err
	}
	return &Store{engine: e}, nil
}

// Put добавляет/обновляет пару (key, value).
func (s *Store) Put(ctx context.Context, key, value []byte) error {
	return s.engine.Put(ctx, key, value)
}

// Get возвращает значение по ключу. Возвращает kv.ErrNotFound, если ключа нет.
func (s *Store) Get(ctx context.Context, key []byte) ([]byte, error) {
	val, err := s.engine.Get(ctx, key)
	if err != nil {
		// Превращаем lsm.ErrNotFound в общий kv.ErrNotFound — это контракт kv.Store.
		if errors.Is(err, lsm.ErrNotFound) {
			return nil, kv.ErrNotFound
		}
		return nil, err
	}
	return val, nil
}

// Delete помечает ключ как удалённый. На уровне Store Delete несуществующего ключа — не ошибка.
func (s *Store) Delete(ctx context.Context, key []byte) error {
	return s.engine.Delete(ctx, key)
}

// Scan возвращает упорядоченный итератор по диапазону [start, end).
// Сейчас Scan не реализован в lsm.Engine — возвращаем kv.ErrNotImplemented.
// Это допустимо: интерфейс kv.Store его требует, но тесты Дня 2 его не используют.
func (s *Store) Scan(ctx context.Context, start, end []byte) (kv.Iterator, error) {
	return nil, kv.ErrNotImplemented
}

// Close завершает работу стора (флаш Memtable, закрытие WAL и SSTable).
func (s *Store) Close() error {
	return s.engine.Close()
}

// Compile-time проверка: *Store удовлетворяет интерфейсу kv.Store.
var _ kv.Store = (*Store)(nil)
