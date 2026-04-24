//go:build day1

package skiplist

import (
	"bytes"
	"testing"
)

func TestSkipList_BasicCRUD(t *testing.T) {
	sl := New(1)

	if err := sl.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := sl.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := sl.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("Put c: %v", err)
	}

	v, err := sl.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get a: %v", err)
	}
	if !bytes.Equal(v, []byte("1")) {
		t.Fatalf("Get a mismatch: %q", string(v))
	}

	if err := sl.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete b: %v", err)
	}
	_, err = sl.Get([]byte("b"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestSkipList_ScanOrderAndRange(t *testing.T) {
	sl := New(1)
	_ = sl.Put([]byte("a"), []byte("1"))
	_ = sl.Put([]byte("b"), []byte("2"))
	_ = sl.Put([]byte("c"), []byte("3"))

	it, err := sl.Scan([]byte("b"), []byte("d"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer it.Close()

	var keys []string
	for {
		k, _, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		keys = append(keys, string(k))
	}
	if len(keys) != 2 || keys[0] != "b" || keys[1] != "c" {
		t.Fatalf("unexpected keys: %#v", keys)
	}
}
func TestSkipList_PutUpdatesValue(t *testing.T) {
	sl := New(1)

	_ = sl.Put([]byte("imsi"), []byte("v1"))
	_ = sl.Put([]byte("imsi"), []byte("v2"))
	_ = sl.Put([]byte("imsi"), []byte("v3"))

	v, err := sl.Get([]byte("imsi"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(v, []byte("v3")) {
		t.Fatalf("expected latest value v3, got %q", v)
	}
}

func TestSkipList_DeleteMissing(t *testing.T) {
	sl := New(1)
	_ = sl.Put([]byte("a"), []byte("1"))

	if err := sl.Delete([]byte("zzz")); err != ErrNotFound {
		t.Fatalf("delete of missing key: expected ErrNotFound, got %v", err)
	}

	// Повторное удаление того же ключа — тоже ErrNotFound.
	_ = sl.Delete([]byte("a"))
	if err := sl.Delete([]byte("a")); err != ErrNotFound {
		t.Fatalf("double delete: expected ErrNotFound, got %v", err)
	}
}

func TestSkipList_GetMissing(t *testing.T) {
	sl := New(1)
	_ = sl.Put([]byte("a"), []byte("1"))

	if _, err := sl.Get([]byte("b")); err != ErrNotFound {
		t.Fatalf("missing key: expected ErrNotFound, got %v", err)
	}

	// Поиск в пустом списке.
	empty := New(1)
	if _, err := empty.Get([]byte("a")); err != ErrNotFound {
		t.Fatalf("empty list: expected ErrNotFound, got %v", err)
	}
}

func TestSkipList_EmptyKeyAndValue(t *testing.T) {
	sl := New(1)

	// Пустое значение — допустимо, отличается от "ключа нет".
	if err := sl.Put([]byte("k"), []byte{}); err != nil {
		t.Fatalf("Put empty value: %v", err)
	}
	v, err := sl.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after empty put: %v", err)
	}
	if len(v) != 0 {
		t.Fatalf("expected empty value, got %q", v)
	}

	// Пустой ключ — тоже валидный []byte.
	if err := sl.Put([]byte{}, []byte("empty-key-val")); err != nil {
		t.Fatalf("Put empty key: %v", err)
	}
	v, err = sl.Get([]byte{})
	if err != nil {
		t.Fatalf("Get empty key: %v", err)
	}
	if !bytes.Equal(v, []byte("empty-key-val")) {
		t.Fatalf("empty key value mismatch: %q", v)
	}
}

func TestSkipList_ScanNilBounds(t *testing.T) {
	sl := New(1)
	for _, k := range []string{"b", "d", "a", "c", "e"} {
		_ = sl.Put([]byte(k), []byte(k))
	}

	// start=nil, end=nil → весь список по порядку.
	got := collect(t, sl, nil, nil)
	want := []string{"a", "b", "c", "d", "e"}
	if !equal(got, want) {
		t.Fatalf("full scan: got %v, want %v", got, want)
	}

	// start=nil, end="c" → [a, b].
	got = collect(t, sl, nil, []byte("c"))
	want = []string{"a", "b"}
	if !equal(got, want) {
		t.Fatalf("scan(nil, c): got %v, want %v", got, want)
	}

	// start="c", end=nil → [c, d, e].
	got = collect(t, sl, []byte("c"), nil)
	want = []string{"c", "d", "e"}
	if !equal(got, want) {
		t.Fatalf("scan(c, nil): got %v, want %v", got, want)
	}
}

func TestSkipList_ScanEmptyAndInvertedRange(t *testing.T) {
	sl := New(1)
	_ = sl.Put([]byte("a"), []byte("1"))
	_ = sl.Put([]byte("b"), []byte("2"))
	_ = sl.Put([]byte("c"), []byte("3"))

	// Пустой диапазон [b, b).
	if got := collect(t, sl, []byte("b"), []byte("b")); len(got) != 0 {
		t.Fatalf("empty range [b,b): expected nothing, got %v", got)
	}

	// Инвертированный диапазон [c, a) — тоже пусто.
	if got := collect(t, sl, []byte("c"), []byte("a")); len(got) != 0 {
		t.Fatalf("inverted range [c,a): expected nothing, got %v", got)
	}

	// Диапазон полностью вне данных.
	if got := collect(t, sl, []byte("x"), []byte("z")); len(got) != 0 {
		t.Fatalf("out-of-range [x,z): expected nothing, got %v", got)
	}
}

func TestSkipList_ScanAfterDelete(t *testing.T) {
	sl := New(1)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		_ = sl.Put([]byte(k), []byte(k))
	}

	_ = sl.Delete([]byte("c"))

	got := collect(t, sl, nil, nil)
	want := []string{"a", "b", "d", "e"}
	if !equal(got, want) {
		t.Fatalf("scan after delete: got %v, want %v", got, want)
	}
}

// --- вспомогательные функции ---

func collect(t *testing.T, sl *SkipList, start, end []byte) []string {
	t.Helper()
	it, err := sl.Scan(start, end)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer it.Close()

	var keys []string
	for {
		k, _, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		keys = append(keys, string(k))
	}
	return keys
}

func equal(a, b []string) bool {
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
