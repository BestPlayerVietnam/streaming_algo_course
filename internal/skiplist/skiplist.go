package skiplist

import (
	"bytes"
	"errors"
	"math/rand"
)

var ErrNotFound = errors.New("skiplist: ключ не найден")

const (
	maxLevel    = 32  // log2(2^32) — хватит для 4 млрд ключей
	probability = 0.8 // вероятность "подъёма" на следующий уровень
)

// node — узел SkipList. forward[i] указывает на следующий узел на уровне i.
type node struct {
	key     []byte
	value   []byte
	forward []*node
}

// Iterator — упорядоченная итерация по диапазону ключей (Range Scan).
// В HLR используется для выгрузки абонентов по префиксу IMSI.
type Iterator interface {
	Next() (key, value []byte, ok bool, err error)
	Close() error
}

// SkipList — In-Memory движок для HLR.
// Обеспечивает O(log N) на чтение/запись и упорядоченный доступ.
type SkipList struct {
	head  *node      // сентинел-узел с forward на maxLevel
	level int        // текущий максимальный занятый уровень (1..maxLevel)
	rng   *rand.Rand // детерминированный RNG из seed
	size  int        // количество ключей
}

// New создаёт SkipList. seed требуется для детерминируемых тестов (воспроизводимость поведения при ошибках).
func New(seed int64) *SkipList {
	return &SkipList{
		head: &node{
			forward: make([]*node, maxLevel),
		},
		level: 1,
		rng:   rand.New(rand.NewSource(seed)),
	}
}

// randomLevel возвращает высоту нового узла по геометрическому распределению.
// С вероятностью p узел "поднимается" на следующий уровень.
// Ожидаемая высота: 1/(1-p). Для p=0.5 это 2, для p=0.25 — 1.33.
func (s *SkipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && s.rng.Float64() < probability {
		lvl++
	}
	return lvl
}

func (s *SkipList) Put(key, value []byte) error {
	// update[i] — узел, после которого должен встать новый узел на уровне i.
	var update [maxLevel]*node
	x := s.head

	// Фаза 1: спуск сверху вниз, ищем позицию вставки.
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}

	// Фаза 2: если ключ уже есть — обновляем значение и выходим.
	if next := x.forward[0]; next != nil && bytes.Equal(next.key, key) {
		next.value = append(next.value[:0], value...) // защитная копия
		return nil
	}

	// Фаза 3: вставка нового узла.
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	// Защитные копии key и value — не храним ссылки на чужие буферы.
	keyCopy := append([]byte(nil), key...)
	valueCopy := append([]byte(nil), value...)
	// buf := make([]byte, len(key)+len(value))
	// copy(buf, key)
	// copy(buf[len(key):], value)
	// keyCopy := buf[:len(key)]
	// valueCopy := buf[len(key):]

	newNode := &node{
		key:     keyCopy,
		value:   valueCopy,
		forward: make([]*node, lvl),
	}
	for i := 0; i < lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	s.size++
	return nil
}

func (s *SkipList) Get(key []byte) ([]byte, error) {
	x := s.head

	// Спуск сверху вниз: на каждом уровне идём вправо, пока можно.
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
	}

	// После спуска x.forward[0] — первый узел с key >= искомого.
	candidate := x.forward[0]
	if candidate != nil && bytes.Equal(candidate.key, key) {
		return candidate.value, nil
	}
	return nil, ErrNotFound
}

func (s *SkipList) Delete(key []byte) error {
	var update [maxLevel]*node
	x := s.head

	// Фаза 1: спуск и запоминание предшественников (как в Put).
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}

	// Фаза 2: проверяем, что искомый узел действительно существует.
	target := x.forward[0]
	if target == nil || !bytes.Equal(target.key, key) {
		return ErrNotFound
	}

	// Фаза 3: перелинковываем предшественников в обход target.
	for i := 0; i < s.level; i++ {
		if update[i].forward[i] != target {
			break // выше этого уровня target уже нет, дальше идти бессмысленно
		}
		update[i].forward[i] = target.forward[i]
	}

	// Фаза 4: ужимаем s.level, если верхние уровни опустели.
	for s.level > 1 && s.head.forward[s.level-1] == nil {
		s.level--
	}

	s.size--
	return nil
}

// scanIterator — ленивый итератор по диапазону [start, end).
// Не копирует данные заранее, двигается по уровню 0 на каждом Next().
type scanIterator struct {
	current *node  // следующий узел для выдачи
	end     []byte // верхняя граница (исключительно); nil означает +∞
	closed  bool
}

func (it *scanIterator) Next() (key, value []byte, ok bool, err error) {
	if it.closed {
		return nil, nil, false, errors.New("skiplist: iterator closed")
	}
	if it.current == nil {
		return nil, nil, false, nil
	}
	if it.end != nil && bytes.Compare(it.current.key, it.end) >= 0 {
		return nil, nil, false, nil
	}
	key = it.current.key
	value = it.current.value
	it.current = it.current.forward[0]
	return key, value, true, nil
}

func (it *scanIterator) Close() error {
	it.closed = true
	it.current = nil
	return nil
}

func (s *SkipList) Scan(start, end []byte) (Iterator, error) {
	// Пустой диапазон: start >= end при обеих заданных границах.
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return &scanIterator{current: nil, end: end}, nil
	}

	x := s.head

	// Если start задан — спускаемся к первому узлу с key >= start.
	// Если start == nil — стартуем с начала списка (head.forward[0]).
	if start != nil {
		for i := s.level - 1; i >= 0; i-- {
			for x.forward[i] != nil && bytes.Compare(x.forward[i].key, start) < 0 {
				x = x.forward[i]
			}
		}
	}

	return &scanIterator{
		current: x.forward[0],
		end:     end,
	}, nil
}
