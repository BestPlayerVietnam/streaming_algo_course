//go:build day1

package skiplist

import (
	"fmt"
	"math/rand"
	"testing"
)

// buildKeys готовит детерминированный набор ключей.
// Формат IMSI-подобный: 15 цифр.
func buildKeys(n int, seed int64) [][]byte {
	r := rand.New(rand.NewSource(seed))
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("%015d", r.Int63n(1_000_000_000_000_000)))
	}
	return keys
}

func BenchmarkPut(b *testing.B) {
	keys := buildKeys(b.N, 42)
	value := []byte("subscriber_data_payload")
	sl := New(1)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = sl.Put(keys[i], value)
	}
}

func BenchmarkGet(b *testing.B) {
	const N = 100_000
	keys := buildKeys(N, 42)
	value := []byte("subscriber_data_payload")

	sl := New(1)
	for _, k := range keys {
		_ = sl.Put(k, value)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = sl.Get(keys[i%N])
	}
}

func BenchmarkScan(b *testing.B) {
	const N = 100_000
	keys := buildKeys(N, 42)
	value := []byte("subscriber_data_payload")

	sl := New(1)
	for _, k := range keys {
		_ = sl.Put(k, value)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		it, _ := sl.Scan(nil, nil)
		for {
			_, _, ok, _ := it.Next()
			if !ok {
				break
			}
		}
		it.Close()
	}
}
