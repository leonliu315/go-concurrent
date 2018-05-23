package concurrent

import (
	"errors"
	//"log"
	"sync"
)

const (
	DEFAULT_INITIAL_CAPACITY  = 16
	DEFAULT_LOAD_FACTOR       = 0.75
	DEFAULT_CONCURRENCY_LEVEL = 16
	MAXIMUM_CAPACITY          = 1 << 30
	MAX_SEGMENTS              = 1 << 16
	RETRIES_BEFORE_LOCK       = 2
)

////////////////////////////////////////////////////////////////////////////////

func WJhash(h int32) int32 {
	// Spread bits to regularize both segment and index locations,
	// using variant of single-word Wang/Jenkins hash.
	var d uint32
	d = 0xffffcd7d
	h += (h << 15) ^ int32(d)
	h ^= int32(uint32(h) >> 10)
	h += (h << 3)
	h ^= int32(uint32(h) >> 6)
	h += (h << 2) + (h << 14)
	h ^= int32(uint32(h) >> 16)
	//return h & 0x7fffffff
	return h
}

type Segment struct {
	count      int
	threshold  int     //增长阈值
	loadFactor float64 //增长因子
	Elements   []*ConcurrentSkipList
	lock       sync.Mutex
}

func NewSegment(initialCapacity int32, lf float64) *Segment {
	sg := new(Segment)
	sg.loadFactor = lf
	sg.Elements = make([]*ConcurrentSkipList, initialCapacity)
	sg.threshold = int(float64(initialCapacity) * lf)

	return sg
}

func (self *Segment) getFirst(h int32) *ConcurrentSkipList {
	elems := self.Elements
	return elems[h&int32(len(elems)-1)]
}

func (self *Segment) SegGet(key KeyFace, hashv int32) (value interface{}, err error) {
	if self.count != 0 {
		e := self.getFirst(hashv)
		if e != nil {
			value, err = e.Get(key)
		}
	} else {
		err = errors.New("there is no element!")
	}
	return
}

func (self *Segment) SegPut(key KeyFace, hashv int32, value interface{}) (old_value interface{}, err error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if value == nil {
		err = errors.New("SegPut: value is nil!")
		return
	}

	c := self.count + 1
	if c > self.threshold {
		err = self.rehash() //存储空间增长
		if err != nil {
			panic(err.Error())
		}
	}

	elems := self.Elements
	index := hashv & int32(len(elems)-1)
	e := elems[index]
	if e != nil {
		old_value, err = e.UnsafePut(key, value)
		if old_value != nil {
			self.count = c
		}
	} else {
		//e = NewConcurrentSkipList()
		e = new(ConcurrentSkipList)
		e.InitWithLevel(3)
		e.UnsafePut(key, value)
		elems[index] = e
		self.count = c
	}
	return
}

func (self *Segment) rehash() (err error) {

	old_elems := self.Elements
	oldCapacity := len(old_elems)
	if oldCapacity >= MAXIMUM_CAPACITY {
		err = errors.New("Capacity over MAXIMUM_CAPACITY!")
		return
	}

	new_elems := make([]*ConcurrentSkipList, oldCapacity<<1)
	self.threshold = int(float64(len(new_elems)) * self.loadFactor)
	sizeMask := int32(len(new_elems) - 1)
	for i := 0; i < oldCapacity; i++ {
		e := old_elems[i]
		if e != nil {
			iter := e.NewConnSkipListIterator()
			for iter.HasNext() {
				entry := iter.Next()
				h := WJhash(int32(entry.Key.HashCode()))
				idx := h & sizeMask
				tmpelem := new_elems[idx]
				if tmpelem != nil {
					tmpelem.UnsafePut(entry.Key, entry.Value)
				} else {
					//tmpelem = NewConcurrentSkipList()
					tmpelem = new(ConcurrentSkipList)
					tmpelem.InitWithLevel(3)
					tmpelem.UnsafePut(entry.Key, entry.Value)
					new_elems[idx] = tmpelem
				}
			}
		}
	}
	self.Elements = new_elems
	return
}

func (self *Segment) SegRemove(key KeyFace, hashv int32) (old_value interface{}, err error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	c := self.count - 1
	elems := self.Elements
	index := hashv & int32(len(elems)-1)
	e := elems[index]
	if e != nil {
		old_value, err = e.UnsafeRemove(key)
		if old_value != nil {
			self.count = c
		}
	}

	return
}

func (self *Segment) GetCount() int {
	return self.count
}

////////////////////////////////////////////////////////////////////////////////

type ConcurrentHashMap struct {
	Segments     []*Segment
	segmentMask  int32
	segmentShift int32
}

func NewConcurrentHashMap(initialCapacity int32) *ConcurrentHashMap {
	ret := new(ConcurrentHashMap)
	ret.Init(initialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL)
	return ret
}

func (self *ConcurrentHashMap) Init(initialCapacity int32, loadFactor float64, concurrencyLevel int32) {
	if initialCapacity <= 0 && loadFactor <= 0 && concurrencyLevel <= 0 {
		panic("error init data!")
	}

	if concurrencyLevel > MAX_SEGMENTS { //并发等级，段数量
		concurrencyLevel = MAX_SEGMENTS
	}

	sshift := int32(0)
	ssize := int32(1)
	for ssize < concurrencyLevel {
		sshift++
		ssize <<= 1
	}

	self.segmentShift = 32 - sshift
	self.segmentMask = ssize - 1

	self.Segments = make([]*Segment, ssize)

	if initialCapacity > MAXIMUM_CAPACITY {
		initialCapacity = MAXIMUM_CAPACITY
	}

	c := initialCapacity / ssize
	if c*ssize < initialCapacity {
		c++
	}

	tmpcap := int32(1)
	for tmpcap < c {
		tmpcap <<= 1
	}

	for i := int32(0); i < ssize; i++ {
		self.Segments[i] = NewSegment(tmpcap, loadFactor)
	}

}

func (self *ConcurrentHashMap) Put(key KeyFace, value interface{}) (old_value interface{}, err error) {
	if key == nil || value == nil {
		err = errors.New("key or value is nil!")
		return
	}
	h := WJhash(int32(key.HashCode()))
	return self.segmentFor(h).SegPut(key, h, value)
}

func (self *ConcurrentHashMap) Get(key KeyFace) (value interface{}, err error) {
	if key == nil {
		err = errors.New("key is nil!")
		return
	}
	h := WJhash(int32(key.HashCode()))
	return self.segmentFor(h).SegGet(key, h)
}

func (self *ConcurrentHashMap) Remove(key KeyFace) (value interface{}, err error) {
	if key == nil {
		err = errors.New("key is nil!")
		return
	}
	h := WJhash(int32(key.HashCode()))
	return self.segmentFor(h).SegRemove(key, h)
}

func (self *ConcurrentHashMap) segmentFor(h int32) *Segment {
	return self.Segments[int32(uint32(h)>>uint32(self.segmentShift))&self.segmentMask]
}

func (self *ConcurrentHashMap) Len() int { //弱一致性
	var sum int
	for i := 0; i < len(self.Segments); i++ {
		sum = sum + self.Segments[i].GetCount()
	}
	return sum
}

func (self *ConcurrentHashMap) Size() int { //遗留方法
	return self.Len()
}

////////////////////////////////////////////////////////////////////////////////

type ConcurrentHashMapIterator struct { //迭代器
	segments   []*Segment
	segms_seek int
	elems      []*ConcurrentSkipList
	elems_seek int
	elemiter   Iterator
}

func (self *ConcurrentHashMap) NewConcurrentHashMapIterator() Iterator {
	cmi := new(ConcurrentHashMapIterator)
	for i := 0; i < len(self.Segments); i++ {
		if self.Segments[i].GetCount() != 0 { //过滤出有数据的段
			cmi.segments = append(cmi.segments, self.Segments[i])
		}
	}
	cmi.segms_seek = 0
	cmi.elems_seek = -1
	return cmi
}

func (self *ConcurrentHashMapIterator) HasNext() bool {

	if len(self.segments) == 0 {
		return false
	}

	if self.elemiter != nil {
		if self.elemiter.HasNext() {
			return true
		}
	}

	var elem *ConcurrentSkipList
	for elem == nil {

		if self.segms_seek >= len(self.segments) {
			return false
		}

		if self.elems_seek == -1 {
			self.elems = self.segments[self.segms_seek].Elements
			self.elems_seek = 0
		}

		for ; self.elems_seek < len(self.elems); self.elems_seek++ {
			elem = self.elems[self.elems_seek]
			if elem != nil {
				if elem.Len() == 0 {
					continue
				}
				self.elems_seek++ //无论有无数据都要向后移动指针
				break
			}

		}

		if self.elems_seek == len(self.elems) {
			self.segms_seek++
			self.elems_seek = -1
		}

	}

	if elem != nil {
		if elem.Len() > 0 {
			self.elemiter = elem.NewConnSkipListIterator()
			return true
		}
	}

	return false
}

func (self *ConcurrentHashMapIterator) Next() *Entry {

	var entry *Entry
	if self.elemiter != nil {
		entry = self.elemiter.Next()
	}

	return entry
}
