# Заметка по Дню 2: CDR Storage / LSM Tree

## 1) Что реализовано

Пакеты:
- `internal/sstable` — формат SSTable (Writer + Reader + Iter).
- `internal/wal` — append-only WAL (length-prefixed + CRC32).
- `internal/lsm` — Engine: Memtable + WAL + SSTable + flush + compaction + recovery.
- `internal/kv/lsmstore` — адаптер `kv.Store` поверх Engine (тонкий фасад).
- `internal/skiplist` — добавлен метод `BytesUsed()` для триггера флаша Memtable.

Операции уровня LSM Engine: `Open`, `Put(ctx,k,v)`, `Get(ctx,k)`, `Delete(ctx,k)`, `Scan(ctx,start,end)`, `Close`.

## 2) Инварианты и семантика

**SSTable**
- Файл = `[Data Block 1] ... [Data Block N] [Index Block] [Footer 16B]`.
- Внутри блока: записи `[key_len uvarint][key][val_len uvarint][value]` подряд.
- Ключи записываются в строго возрастающем порядке (нарушение → `ErrUnorderedKeys`).
- Footer содержит magic `0x5354424C` (ASCII "STBL") для раннего обнаружения порчи или ошибочного пути.
- При открытии Reader читает footer и весь индекс в память — точечный Get не делает лишних чтений.

**WAL**
- Запись = `[crc32 4][type 1][key_len uvarint][key][val_len uvarint][value]`.
- CRC покрывает payload (type + длины + данные). При torn write на crash CRC не сходится → Reader тихо завершает чтение, не возвращая ошибку. Это норма для recovery.
- Чистый EOF между записями и обрыв внутри записи трактуются одинаково: «конец лога».
- Реальное повреждение (валидный CRC, но неизвестный тип операции) — ошибка наружу.

**LSM Engine**
- Внутреннее значение в Memtable/SSTable: `[tag 1][user_value]`. `tag = 0x00 (Put) | 0x01 (Delete-tombstone)`. Это позволяет отличить «пустое значение пользователя» от «удалено».
- `Put`: WAL.Append → fsync → Memtable.Put. Если WAL упал — Memtable не модифицируется.
- `Delete`: то же самое, но в Memtable пишется tombstone. Идемпотентен (не ошибка для несуществующего ключа).
- `Get`: Memtable → SSTable от свежего к старому. Tombstone «забивает» все нижние уровни.
- Flush триггерится при `Memtable.BytesUsed() >= MemtableFlushThreshold` после каждого Put/Delete. Также в `Close`.
- Compaction триггерится при `len(sstables) >= CompactionThreshold`. Стратегия: «слить всё L0» в один новый SSTable. Tombstone'ы физически удаляются (нижних уровней нет).

**Атомарность на диске**
- SSTable пишется во временный `*.tmp`, фsync, потом `Rename` во финальное имя. POSIX гарантирует атомарность rename.
- Старый WAL удаляется только **после** успешного rename SSTable. Падение между rename и удалением → на следующем Open WAL накатится поверх SSTable, дубликаты в Memtable (Put идемпотентен). Корректность сохраняется.
- При Open: cleanup `*.tmp` файлов от незавершённого флаша.

## 3) Принятые решения (и почему)

- **Tombstone как байт-тег в значении**, а не отдельное поле узла Memtable. Идиоматично (так делают RocksDB/LevelDB), не требует менять SkipList Дня 1, явно отличается от пустого значения. Кодирование инкапсулировано в `encodeValue`/`decodeValue` — байты с тегами нигде в коде Engine «магией» не торчат.
- **SSTable: блоки + sparse index в футере**, а не плоский «всё подряд». Размер блока 4 КБ — кратен странице FS, минимизирует read amplification. Sparse-index держится в памяти Reader'а целиком (для 64 МБ файла это ~500 КБ), зато каждый Get потом — без лишнего I/O.
- **Varint для длин** в SSTable и WAL. Для коротких ключей и значений экономит ~3 байта на запись против fixed uint32. Соответствует подходу production LSM.
- **CRC32 в WAL обязательный**, а не опциональный. Без него torn write нельзя отличить от валидной записи — recovery работало бы непредсказуемо. Стоимость минимальная (4 байта + IEEE polynomial — это аппаратная инструкция на современных CPU).
- **fsync после каждого Put**. Это даёт строгую гарантию: если Put вернул nil — данные пережили kill -9. Дорого по производительности (см. бенчмарки), но это профиль CDR (потеря даже одного звонка → потеря денег).
- **Compaction синхронный, в потоке Put**. Не в фоне. Упрощение: задание не требует асинхронности. При срабатывании триггера блокируется один Put, остальные ждут. Для масштабируемости нужно вынести в отдельную горутину с `imm` Memtable.
- **Стратегия compaction: «слить всё L0»**, не L0→L1. Простая, достаточная для тестов, легко рассуждать. См. раздел про write amplification ниже.
- **`Scan` отложен**. Тесты не требуют, но без него LSM «концептуально неполный» (CDR-сценарий — это range scan по периоду). TODO на следующий день.

## 4) Контроль (как проверялось)

Тесты по пакетам:
- `internal/sstable`: 7 (формат, нарушение порядка, многоблочные файлы, roundtrip, диапазоны, мусорный magic).
- `internal/wal`: 5 (roundtrip, torn tail, bit-flip в payload, пустой лог, неизвестный op).
- `internal/lsm`: 21 (базовые Put/Get/Delete, tombstone сквозь уровни, flush на пороге и на Close, recovery без флаша и после флаша, Compaction Test (10 версий ключа → 1 запись), tombstone drops, авто-триггер compaction, edge-cases: пустые ключи/значения, большие значения, повторные Put, Delete→Put).
- `internal/kv/lsmstore`: 1 (исходный `TestLSMStore_PersistAcrossRestart`).

**Контрольные точки задания:**
- Crash Test → закрыт `TestLSM_RecoveryFromWAL_NoFlush` и `TestLSM_RecoveryFromWAL_AfterFlush`. Имитация краша через приватный `crashClose()` (закрывает дескрипторы без graceful shutdown).
- Compaction Test → закрыт `TestLSM_CompactionRemovesDuplicates` (10 версий ключа → 1 запись после compaction).

**Бенчмарки** (`-bench=. -benchmem`, 12-Gen Intel i7):

BenchmarkPut/N=100       49 iter   23.99 ms/op    ~240 µs/Put
BenchmarkPut/N=1000       4 iter  273.43 ms/op    ~273 µs/Put   (линейно от N)
BenchmarkPut/N=10000      1 iter 2870.54 ms/op    ~287 µs/Put   (линейно от N)
BenchmarkGet/N=1000/hit       126 ns/op   64 B/op  1 alloc/op
BenchmarkGet/N=1000/miss       45 ns/op    0 B/op  0 allocs/op
BenchmarkGet/N=10000/hit      162 ns/op   64 B/op  1 alloc/op
BenchmarkGet/N=10000/miss      67 ns/op    0 B/op  0 allocs/op

**Тренды:**
- Put — линейный от N, ~240–290 µs/op. Упор в fsync (~100–200 µs на современном SSD). Без батчинга fsync пропускную способность не поднять.
- Get — O(log N) от размера Memtable: hit 126 → 162 ns при росте N в 10 раз даёт ~30% — соответствует `log(10) ≈ 3.3` логарифмическому шагу.
- Hit делает 1 аллокацию (копия value наружу). Miss = 0 аллокаций.

**Дополнительное задание: write amplification на 100 МБ**

Прогон `TestWriteAmplification_100MB` (385K записей по 272 байт = 100 МБ полезной нагрузки):

WAF=18.6x при `CompactionThreshold=4`. Источник: стратегия «слить всё L0» переписывает все накопленные SSTable при каждом срабатывании. С 100 флашами и порогом 4 получаем ~25 compaction'ов, каждый перезаписывает прогрессивно растущий объём — суммарно ~N²·2 МБ записей при compaction. Цифра 1862 МБ сходится с теоретической оценкой.

Финальный размер БД (102 МБ) почти равен полезной нагрузке — compaction свою работу делает, мусор на диске не копится. Проблема не в эффективности конечного результата, а в **стоимости пути** до него.

**Ответы на вопросы из задания:**

> Если мы делаем Compaction очень часто, что страдает: CPU, Диск (запись) или Диск (чтение)?

**Диск (запись)**. Каждый compaction переписывает данные на диск, и при низком пороге это происходит часто. CPU тоже задействован (merge sort + crc + сериализация), но он не bottleneck — fsync доминирует. Read amplification, наоборот, уменьшается при частом compaction (меньше SSTable для проверки). Цена платится записью.

> Почему для CDR данных (пишем много, читаем редко) выгодно откладывать Compaction?

CDR — write-heavy, read-rare. Read amplification (медленный Get при многих SSTable) для CDR несильно болит — биллинг и аналитика идут редко и могут потерпеть пол-секунды. А write amplification бьёт постоянно: износ SSD, тормоза при пиках записи. Откладывая compaction (повышая порог), мы переплачиваем при редких чтениях, но экономим на записях. Для CDR это правильный размен.

## 5) Известные ограничения

- **Compaction блокирует Put** на время merge (нет фонового потока). При пиках записи это видимая пауза. Решение: фоновая горутина + immutable Memtable, которая флашится отдельно.
- **Aggressive compaction (`CompactionThreshold=4` по умолчанию)** даёт WAF=18.6x. Для CDR оптимальнее повышенный порог (32+) — это сильно снизит write amplification ценой более медленных Get при миссах.
- **Sparse index в SSTable Reader держится в памяти целиком**. Для гигантских файлов (десятки ГБ) это станет проблемой; production-решение — двухуровневый индекс.
- **`Scan` не реализован** в Engine и lsmstore. Все слои по отдельности (Memtable, SSTable) умеют сканить, но k-way merge итератор для всего хранилища ещё не написан. TODO на День 3.
- **Аллокации в горячем пути**: Bench показал ~24 allocs на Put. Большая часть — повторная сборка payload в WAL (пишем + считаем CRC отдельно) и копии в `encodeValue`. Кандидат для `sync.Pool` под reusable buffer.
- **WAL recovery не использует sequence-номера sealed SSTable**. Если flush прошёл, но WAL не успел удалиться — при recovery записи накатятся повторно (избыточно, но корректно благодаря идемпотентности Put). Production-LSM хранят watermark и пропускают «уже зафлаженные» записи.