# DAY 1 
go test -tags=day1 -v ./...  - тесты
go run ./cmd/kvtool load -count 10000 - бенчи
go run ./cmd/kvtool wordcount -in ./testdata/text_small.txt -store skiplist - подключить скиплист

cd internal/skiplist
go test -tags=day1 -bench=BenchmarkPut -benchmem -benchtime=2s - запуск тест_бенча

# DAY 2
go clean -testcache   - Почистить кэш

go test -tags=day2 '-bench=.' -benchmem '-run=^$' . - Бенчмарки
go test -tags=day2 -v -run TestLSM . - Тесты
go test -tags=day2 ./... - базовые тесты
go test -tags=day2 -run TestWriteAmplification_100MB -v ./internal/lsm/ - write амплифай

