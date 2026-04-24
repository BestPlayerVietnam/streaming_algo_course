go test -tags=day1 -v ./...  - тесты
go run ./cmd/kvtool load -count 10000 - бенчи
go run ./cmd/kvtool wordcount -in ./testdata/text_small.txt -store skiplist - подключить скиплист

cd internal/skiplist
go test -tags=day1 -bench=BenchmarkPut -benchmem -benchtime=2s - запуск тест_бенча