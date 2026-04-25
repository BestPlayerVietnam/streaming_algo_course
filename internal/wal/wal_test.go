//go:build day2

package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestWAL_RoundtripBasic(t *testing.T) {
	records := []Record{
		{Type: OpPut, Key: []byte("a"), Value: []byte("1")},
		{Type: OpPut, Key: []byte("b"), Value: []byte("22")},
		{Type: OpDelete, Key: []byte("a"), Value: nil},
		{Type: OpPut, Key: []byte("c"), Value: []byte("")},
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, r := range records {
		if err := w.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()))
	var got []Record
	for {
		rec, ok, err := r.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, rec)
	}

	if len(got) != len(records) {
		t.Fatalf("записей: got=%d want=%d", len(got), len(records))
	}
	for i := range records {
		if got[i].Type != records[i].Type {
			t.Errorf("rec[%d].Type: got=%v want=%v", i, got[i].Type, records[i].Type)
		}
		if !bytes.Equal(got[i].Key, records[i].Key) {
			t.Errorf("rec[%d].Key: got=%q want=%q", i, got[i].Key, records[i].Key)
		}
		// Value у OpDelete можем не сравнивать — он "не несёт смысла".
		if records[i].Type == OpPut && !bytes.Equal(got[i].Value, records[i].Value) {
			t.Errorf("rec[%d].Value: got=%q want=%q", i, got[i].Value, records[i].Value)
		}
	}
}

// TestWAL_TornWrite_TruncatedTail проверяет crash recovery:
// если последняя запись недописана (обрыв payload),
// Reader должен вернуть успешные записи до неё и тихо остановиться.
func TestWAL_TornWrite_TruncatedTail(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.Append(Record{Type: OpPut, Key: []byte("good"), Value: []byte("ok")}); err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if err := w.Append(Record{Type: OpPut, Key: []byte("torn"), Value: []byte("oops")}); err != nil {
		t.Fatalf("Append 2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	full := buf.Bytes()
	// Обрезаем последние 5 байт — это попадает в payload второй записи.
	truncated := full[:len(full)-5]

	r := NewReader(bytes.NewReader(truncated))
	rec, ok, err := r.Next()
	if err != nil || !ok {
		t.Fatalf("первая запись должна быть прочитана, got ok=%v err=%v", ok, err)
	}
	if string(rec.Key) != "good" {
		t.Fatalf("первая запись: key=%q want=%q", rec.Key, "good")
	}

	rec, ok, err = r.Next()
	if err != nil {
		t.Fatalf("вторая запись: ожидали тихое завершение, получили ошибку: %v", err)
	}
	if ok {
		t.Fatalf("вторая запись не должна была вернуться, got=%+v", rec)
	}
}

// TestWAL_BitFlipInPayload — мы вручную портим один байт в payload
// последней записи. CRC должен это поймать и вернуть конец лога без ошибки.
func TestWAL_BitFlipInPayload(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.Append(Record{Type: OpPut, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	corrupted := append([]byte{}, buf.Bytes()...)
	// Портим последний байт (он лежит в value).
	corrupted[len(corrupted)-1] ^= 0xFF

	r := NewReader(bytes.NewReader(corrupted))
	_, ok, err := r.Next()
	if err != nil {
		t.Fatalf("ожидали тихий конец, получили ошибку: %v", err)
	}
	if ok {
		t.Fatal("CRC должен был не сойтись, но запись вернулась как валидная")
	}
}

// TestWAL_EmptyLog — пустой файл должен сразу давать "конец".
func TestWAL_EmptyLog(t *testing.T) {
	r := NewReader(bytes.NewReader(nil))
	_, ok, err := r.Next()
	if err != nil {
		t.Fatalf("ожидали тихий конец, получили: %v", err)
	}
	if ok {
		t.Fatal("ожидали ok=false на пустом логе")
	}
}

// TestWAL_UnknownOp — реальное повреждение (тип не Put/Delete) должно стать ошибкой.
// Конструируем валидную с точки зрения CRC запись с непонятным типом.
func TestWAL_UnknownOp(t *testing.T) {
	// Ручной payload с типом 0x99 (не Put/Delete), key="x", value="".
	var lenBuf [binary.MaxVarintLen64]byte
	keyLenN := binary.PutUvarint(lenBuf[:], 1)
	keyLenHeader := append([]byte(nil), lenBuf[:keyLenN]...)
	valLenN := binary.PutUvarint(lenBuf[:], 0)
	valLenHeader := append([]byte(nil), lenBuf[:valLenN]...)

	payload := []byte{}
	payload = append(payload, 0x99) // невалидный тип
	payload = append(payload, keyLenHeader...)
	payload = append(payload, 'x')
	payload = append(payload, valLenHeader...)
	// value пустой — нечего добавлять

	crc := crc32.Checksum(payload, crc32.MakeTable(crc32.IEEE))

	var rec bytes.Buffer
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc)
	rec.Write(crcBuf[:])
	rec.Write(payload)

	r := NewReader(&rec)
	_, ok, err := r.Next()
	if ok {
		t.Fatal("ожидали false, ok=true")
	}
	if err == nil {
		t.Fatal("ожидали ошибку для неизвестного op")
	}
}
