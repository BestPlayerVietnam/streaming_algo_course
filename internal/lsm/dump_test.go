//go:build day2

package lsm

import (
	"context"
	"fmt"
	"testing"
)

// TestDumpFiles — ручной тест для разглядывания файлов на диске.
// По умолчанию пропускается (t.Skip), запусти явно через -run TestDumpFiles
// чтобы получить файлы в ./debug_db рядом с тестом.
func TestDumpFiles(t *testing.T) {
	t.Skip("ручной тест: убери эту строку и запусти -run TestDumpFiles")

	ctx := context.Background()
	dir := "./debug_db" // относительно internal/lsm/

	e, err := Open(Options{
		Dir:                    dir,
		MemtableFlushThreshold: 4 * 1024,
		CompactionThreshold:    100, // не сжимать — увидим много sstables
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key_%05d", i))
		val := []byte(fmt.Sprintf("value_for_key_%05d_padding_padding_padding", i))
		if err := e.Put(ctx, key, val); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	t.Logf("файлы записаны в: %s", dir)
}
