//go:build day2

package sstable

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestWriter_FormatBasic проверяет, что Writer на простом наборе данных
// производит файл ожидаемого формата:
//   - блок данных в начале
//   - индекс после блока
//   - footer ровно 16 байт в конце с правильным magic
//
// Парсим результат руками — Reader ещё не реализован, поэтому самостоятельно.
func TestWriter_FormatBasic(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	pairs := []struct{ k, v string }{
		{"alpha", "1"},
		{"beta", "22"},
		{"gamma", "333"},
	}
	for _, p := range pairs {
		if err := w.Add([]byte(p.k), []byte(p.v)); err != nil {
			t.Fatalf("Add(%q): %v", p.k, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	if len(data) < footerSize {
		t.Fatalf("файл слишком короткий: %d байт", len(data))
	}

	// 1. Парсим footer (последние 16 байт).
	footer := data[len(data)-footerSize:]
	indexOffset := binary.BigEndian.Uint64(footer[0:8])
	indexLength := binary.BigEndian.Uint32(footer[8:12])
	magic := binary.BigEndian.Uint32(footer[12:16])

	if magic != magicNumber {
		t.Fatalf("magic mismatch: got=%#x want=%#x", magic, magicNumber)
	}
	t.Logf("footer: index_offset=%d index_length=%d", indexOffset, indexLength)

	// 2. Проверяем непротиворечивость: index лежит между data и footer.
	if int(indexOffset)+int(indexLength)+footerSize != len(data) {
		t.Fatalf("неконсистентный layout: indexOffset=%d indexLength=%d footer=%d total=%d",
			indexOffset, indexLength, footerSize, len(data))
	}

	// 3. Парсим index block.
	indexBlock := data[indexOffset : indexOffset+uint64(indexLength)]
	type idxEnt struct {
		firstKey []byte
		offset   uint64
		length   uint32
	}
	var idx []idxEnt
	for pos := 0; pos < len(indexBlock); {
		klen, n := binary.Uvarint(indexBlock[pos:])
		if n <= 0 {
			t.Fatalf("bad uvarint at index pos=%d", pos)
		}
		pos += n
		key := indexBlock[pos : pos+int(klen)]
		pos += int(klen)
		off := binary.BigEndian.Uint64(indexBlock[pos : pos+8])
		pos += 8
		ln := binary.BigEndian.Uint32(indexBlock[pos : pos+4])
		pos += 4
		idx = append(idx, idxEnt{firstKey: key, offset: off, length: ln})
	}
	if len(idx) == 0 {
		t.Fatal("индекс пуст")
	}
	t.Logf("index entries: %d", len(idx))

	// 4. Для трёх маленьких записей всё должно уложиться в один блок,
	//    значит в индексе ровно одна запись с firstKey="alpha".
	if len(idx) != 1 {
		t.Fatalf("ожидали 1 индексную запись (всё помещается в один блок), получили %d", len(idx))
	}
	if string(idx[0].firstKey) != "alpha" {
		t.Fatalf("firstKey: got=%q want=%q", idx[0].firstKey, "alpha")
	}

	// 5. Парсим data block по offset/length из индекса и сверяем с pairs.
	dataBlock := data[idx[0].offset : idx[0].offset+uint64(idx[0].length)]
	var got []struct{ k, v string }
	for pos := 0; pos < len(dataBlock); {
		klen, n := binary.Uvarint(dataBlock[pos:])
		if n <= 0 {
			t.Fatalf("bad uvarint key_len at pos=%d", pos)
		}
		pos += n
		key := dataBlock[pos : pos+int(klen)]
		pos += int(klen)

		vlen, n := binary.Uvarint(dataBlock[pos:])
		if n <= 0 {
			t.Fatalf("bad uvarint val_len at pos=%d", pos)
		}
		pos += n
		val := dataBlock[pos : pos+int(vlen)]
		pos += int(vlen)

		got = append(got, struct{ k, v string }{string(key), string(val)})
	}

	if len(got) != len(pairs) {
		t.Fatalf("записей: got=%d want=%d", len(got), len(pairs))
	}
	for i, p := range pairs {
		if got[i].k != p.k || got[i].v != p.v {
			t.Errorf("pair[%d]: got=(%q,%q) want=(%q,%q)", i, got[i].k, got[i].v, p.k, p.v)
		}
	}
}

// TestWriter_RejectsUnorderedKeys проверяет инвариант "ключи строго возрастающие".
func TestWriter_RejectsUnorderedKeys(t *testing.T) {
	cases := []struct {
		name string
		keys []string
	}{
		{"равные ключи", []string{"a", "a"}},
		{"убывание", []string{"b", "a"}},
		{"нарушение в середине", []string{"a", "c", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := NewWriter(&bytes.Buffer{})
			var lastErr error
			for _, k := range tc.keys {
				lastErr = w.Add([]byte(k), []byte("v"))
				if lastErr != nil {
					break
				}
			}
			if lastErr == nil {
				t.Fatalf("ожидали ошибку нарушения порядка, получили nil")
			}
		})
	}
}

// TestWriter_MultipleBlocks проверяет, что при большом количестве данных
// Writer создаёт несколько блоков, и в индексе несколько записей.
func TestWriter_MultipleBlocks(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	// Записи по ~100 байт; чтобы получить >1 блок при targetBlockSize=4096,
	// нужно > ~40 записей. Возьмём 200 для запаса.
	bigVal := bytes.Repeat([]byte("x"), 100)
	const N = 200
	for i := 0; i < N; i++ {
		// Ключи в формате "key_000", "key_001", ... — строго возрастающие лексикографически.
		key := []byte(formatKey(i))
		if err := w.Add(key, bigVal); err != nil {
			t.Fatalf("Add #%d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	footer := data[len(data)-footerSize:]
	indexOffset := binary.BigEndian.Uint64(footer[0:8])
	indexLength := binary.BigEndian.Uint32(footer[8:12])
	magic := binary.BigEndian.Uint32(footer[12:16])

	if magic != magicNumber {
		t.Fatalf("magic mismatch: %#x", magic)
	}

	indexBlock := data[indexOffset : indexOffset+uint64(indexLength)]
	count := 0
	for pos := 0; pos < len(indexBlock); {
		klen, n := binary.Uvarint(indexBlock[pos:])
		if n <= 0 {
			t.Fatalf("bad uvarint")
		}
		pos += n + int(klen) + 8 + 4
		count++
	}
	if count < 2 {
		t.Fatalf("ожидали несколько блоков (>= 2), получили %d", count)
	}
	t.Logf("получено %d блоков на %d записей", count, N)
}

// formatKey формирует ключ "key_NNN" с лексикографически корректным порядком.
// fmt.Sprintf тут избыточен; используем простую логику для зерокопий не нужно.
func formatKey(i int) string {
	const digits = "0123456789"
	buf := []byte("key_000")
	buf[6] = digits[i%10]
	buf[5] = digits[(i/10)%10]
	buf[4] = digits[(i/100)%10]
	return string(buf)
}

// TestRoundtrip_Basic пишет данные через Writer и читает обратно через Reader.
func TestRoundtrip_Basic(t *testing.T) {
	sortPairs := []struct{ k, v string }{
		{"alpha", "1"}, {"beta", "22"}, {"delta", ""},
		{"epsilon", "long-ish value with several words"}, {"gamma", "333"},
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, p := range sortPairs {
		if err := w.Add([]byte(p.k), []byte(p.v)); err != nil {
			t.Fatalf("Add(%q): %v", p.k, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	it, err := r.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	var got []struct{ k, v string }
	for {
		k, v, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, struct{ k, v string }{string(k), string(v)})
	}

	if len(got) != len(sortPairs) {
		t.Fatalf("записей: got=%d want=%d", len(got), len(sortPairs))
	}
	for i := range sortPairs {
		if got[i] != sortPairs[i] {
			t.Errorf("pair[%d]: got=%v want=%v", i, got[i], sortPairs[i])
		}
	}
}

// TestRoundtrip_Range проверяет диапазонное чтение [start, end).
func TestRoundtrip_Range(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f", "g"}
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, k := range keys {
		if err := w.Add([]byte(k), []byte("v_"+k)); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	cases := []struct {
		name       string
		start, end []byte
		want       []string
	}{
		{"полный скан", nil, nil, []string{"a", "b", "c", "d", "e", "f", "g"}},
		{"[c, f)", []byte("c"), []byte("f"), []string{"c", "d", "e"}},
		{"[a, c)", []byte("a"), []byte("c"), []string{"a", "b"}},
		{"[c, end)", []byte("c"), nil, []string{"c", "d", "e", "f", "g"}},
		{"[start, c)", nil, []byte("c"), []string{"a", "b"}},
		{"пустой диапазон", []byte("c"), []byte("c"), nil},
		{"start больше всех", []byte("z"), nil, nil},
		{"end меньше всех", nil, []byte("0"), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			it, err := r.Iterator(tc.start, tc.end)
			if err != nil {
				t.Fatalf("Iterator: %v", err)
			}
			defer it.Close()

			var got []string
			for {
				k, _, ok, err := it.Next()
				if err != nil {
					t.Fatalf("Next: %v", err)
				}
				if !ok {
					break
				}
				got = append(got, string(k))
			}
			if !equalStrings(got, tc.want) {
				t.Errorf("got=%v want=%v", got, tc.want)
			}
		})
	}
}

// TestRoundtrip_MultiBlock проверяет чтение с пересечением границ блоков.
func TestRoundtrip_MultiBlock(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	bigVal := bytes.Repeat([]byte("x"), 100)
	const N = 200
	for i := 0; i < N; i++ {
		if err := w.Add([]byte(formatKey(i)), bigVal); err != nil {
			t.Fatalf("Add #%d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if len(r.index) < 2 {
		t.Fatalf("ожидали несколько блоков, получили %d", len(r.index))
	}

	// Читаем всё, проверяем количество и порядок.
	it, err := r.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	count := 0
	var prev []byte
	for {
		k, _, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		if prev != nil && bytes.Compare(k, prev) <= 0 {
			t.Fatalf("нарушен порядок: prev=%q k=%q", prev, k)
		}
		prev = k
		count++
	}
	if count != N {
		t.Fatalf("count: got=%d want=%d", count, N)
	}
}

// TestNewReader_RejectsBadMagic проверяет, что Reader не открывает мусорный файл.
func TestNewReader_RejectsBadMagic(t *testing.T) {
	garbage := bytes.Repeat([]byte{0xAA}, 100)
	_, err := NewReader(bytes.NewReader(garbage), int64(len(garbage)))
	if err == nil {
		t.Fatal("ожидали ошибку, получили nil")
	}
}

func equalStrings(a, b []string) bool {
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
