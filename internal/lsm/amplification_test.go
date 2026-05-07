//go:build day2

package lsm

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// TestWriteAmplification_100MB пишет 100 МБ полезных данных мелкими порциями
// и измеряет write amplification: сколько байт реально прошло через диск
// относительно объёма "пользовательских" данных.
//
// Метод: на каждом шаге снимаем размер всех файлов в директории. Если файл
// исчез между снапшотами (compaction удалил), его последний наблюдённый
// размер всё равно засчитан в "когда-либо записано". Это и есть честная
// метрика write amplification.
//
// Запуск:
//
//	go test -tags=day2 -run TestWriteAmplification_100MB -v ./internal/lsm/
func TestWriteAmplification_100MB(t *testing.T) {
	t.Skip("Закомментить когда замерить")
	if testing.Short() {
		t.Skip("долгий тест, пропускается в -short режиме")
	}

	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")

	e, err := Open(Options{
		Dir:                    dir,
		MemtableFlushThreshold: 1 * 1024 * 1024, // 1 MiB как в задании
		CompactionThreshold:    4,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const (
		keySize        = 16
		valueSize      = 256
		totalUserBytes = 100 * 1024 * 1024 // 100 MiB
		recordsPerSnap = 4096              // как часто снимать состояние диска
	)
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i)
	}

	N := totalUserBytes / (keySize + valueSize)
	t.Logf("записываем %d записей по (%d+%d) байт = %d МБ полезной нагрузки",
		N, keySize, valueSize, totalUserBytes/(1024*1024))

	// Снимок: для каждого файла последний наблюдённый размер.
	// Когда файл исчезнет — оставим его максимум в "everSeen".
	maxSeenPerFile := make(map[string]int64)
	updateSnapshot := func() error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		seenNow := make(map[string]bool, len(entries))
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			seenNow[e.Name()] = true
			if sz, ok := maxSeenPerFile[e.Name()]; !ok || info.Size() > sz {
				maxSeenPerFile[e.Name()] = info.Size()
			}
		}
		return nil
	}

	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("key_%012d", i))
		if err := e.Put(ctx, key, value); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
		if i%recordsPerSnap == 0 {
			if err := updateSnapshot(); err != nil {
				t.Fatalf("updateSnapshot: %v", err)
			}
		}
	}

	// Финальный снимок: на момент Close может быть compaction.
	if err := updateSnapshot(); err != nil {
		t.Fatalf("updateSnapshot final: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := updateSnapshot(); err != nil {
		t.Fatalf("updateSnapshot after close: %v", err)
	}

	// Сумма «когда-либо наблюдённых максимумов» — это нижняя оценка реально
	// записанных байт. Реальный объём может быть чуть больше, если файл
	// был перезаписан с уменьшением — но в нашей схеме этого не бывает
	// (SSTable immutable, WAL append-only до ротации).
	var totalBytesWritten int64
	for _, sz := range maxSeenPerFile {
		totalBytesWritten += sz
	}

	// Текущий размер директории — то, что реально лежит на диске сейчас.
	curSize, err := currentDirSize(dir)
	if err != nil {
		t.Fatalf("currentDirSize: %v", err)
	}

	userBytes := int64(N) * int64(keySize+valueSize)

	t.Logf("=== Write Amplification ===")
	t.Logf("Полезная нагрузка (user put):     %s", humanMB(userBytes))
	t.Logf("Записано на диск (всего за время): %s", humanMB(totalBytesWritten))
	t.Logf("Размер БД сейчас (после compact): %s", humanMB(curSize))
	if userBytes > 0 {
		ratio := float64(totalBytesWritten) / float64(userBytes)
		t.Logf("Write Amplification Factor:        %.2fx", ratio)
	}
}

func humanMB(b int64) string {
	mb := float64(b) / (1024 * 1024)
	return fmt.Sprintf("%8.2f MB (%d B)", mb, b)
}

// currentDirSize суммирует размеры всех файлов в каталоге (нерекурсивно).
func currentDirSize(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total, nil
}

// нужно для совместимости — fs.FileInfo алиас, чтобы избежать неиспользуемого импорта.
var _ fs.FileInfo = (os.FileInfo)(nil)
