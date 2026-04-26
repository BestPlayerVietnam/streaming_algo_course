//go:build day2

package lsm

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// BenchmarkPut измеряет стоимость Put при разном масштабе данных.
// Цель: посмотреть, как fsync после каждой записи влияет на пропускную способность,
// и держится ли стоимость Put стабильной по мере роста (или растёт от compaction).
func BenchmarkPut(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			ctx := context.Background()
			val := make([]byte, 256)
			for i := range val {
				val[i] = byte(i)
			}

			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				dir := filepath.Join(b.TempDir(), "db")
				e, err := Open(Options{
					Dir:                    dir,
					MemtableFlushThreshold: 64 * 1024,
					CompactionThreshold:    8,
				})
				if err != nil {
					b.Fatalf("Open: %v", err)
				}
				b.StartTimer()

				for i := 0; i < n; i++ {
					key := keyFromInt(i)
					if err := e.Put(ctx, key, val); err != nil {
						b.Fatalf("Put: %v", err)
					}
				}

				b.StopTimer()
				_ = e.Close()
				b.StartTimer()
			}
		})
	}
}

// BenchmarkGet измеряет стоимость Get при разном размере базы.
// Особенно интересно: как растёт стоимость промахов при увеличении числа SSTable
// (если бы мы убрали compaction — read amplification стал бы виден).
func BenchmarkGet(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("N=%d/hit", n), func(b *testing.B) {
			ctx := context.Background()
			dir := filepath.Join(b.TempDir(), "db")
			e, err := Open(Options{Dir: dir})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer e.Close()

			val := make([]byte, 64)
			for i := 0; i < n; i++ {
				if err := e.Put(ctx, keyFromInt(i), val); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}

			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				key := keyFromInt(iter % n)
				if _, err := e.Get(ctx, key); err != nil {
					b.Fatalf("Get hit failed: %v", err)
				}
			}
		})

		b.Run(fmt.Sprintf("N=%d/miss", n), func(b *testing.B) {
			ctx := context.Background()
			dir := filepath.Join(b.TempDir(), "db")
			e, err := Open(Options{Dir: dir})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer e.Close()

			val := make([]byte, 64)
			for i := 0; i < n; i++ {
				if err := e.Put(ctx, keyFromInt(i), val); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}

			missKey := []byte("zzz_no_such_key")
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				_, _ = e.Get(ctx, missKey) // ErrNotFound — это норма для бенчмарка
			}
		})
	}
}

// keyFromInt — детерминированный лексикографически возрастающий ключ.
func keyFromInt(i int) []byte {
	const digits = "0123456789"
	buf := []byte("key_0000000")
	for pos := 10; pos >= 4; pos-- {
		buf[pos] = digits[i%10]
		i /= 10
	}
	return buf
}
