package concurrent

import (
	"errors"
	"log"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	MAX_LEVEL = 12

	//for random
	M uint32 = 2147483647
	A uint64 = 16807
)

type Random struct {
	seed_ uint32
}

func NewRandom(s uint32) *Random {
	ret := new(Random)
	ret.Init(s)
	return ret
}

func (self *Random) Init(s uint32) {
	self.seed_ = s & 0x7fffffff
}

func (self *Random) Next() uint32 {
	product := uint64(self.seed_) * A
	self.seed_ = uint32((product >> 31) + (product & uint64(M)))
	if self.seed_ > M {
		self.seed_ -= M
	}
	return self.seed_
}

type node struct {
	deleted     int32 //alignment
	fullyLinked int32
	forward     []*node
	prevnode    *node
	key         KeyFace
	value       interface{}
	level       int
	m           Mutex
}

func (self *node) next() *node {
	if self.level == 0 {
		return nil
	}
	return self.forward[0]
}

func (self *node) prev() *node {
	if self.level == 0 {
		return nil
	}
	return self.prevnode
}

func (self *node) hasNext() bool {
	return self.next() != nil
}

func (self *node) Level() int {
	return self.level
}

type ConcurrentSkipList struct {
	tailer *node
	header *node
	length int32
	rng    *Random
}

func NewConcurrentSkipList() *ConcurrentSkipList {
	ret := new(ConcurrentSkipList)
	ret.Init()
	return ret
}

func (self *ConcurrentSkipList) Init() {
	sltail := new(node)
	slhead := new(node)
	sltail.level = MAX_LEVEL
	slhead.level = 1
	sltail.forward = make([]*node, MAX_LEVEL)
	slhead.forward = make([]*node, MAX_LEVEL)
	for i := 0; i < len(slhead.forward); i++ {
		slhead.forward[i] = sltail
	}
	slhead.fullyLinked = 1
	sltail.fullyLinked = 1
	self.header = slhead
	self.tailer = sltail
	self.rng = NewRandom(0xdeadbeef)
}

func (self *ConcurrentSkipList) Len() int32 { //弱一致性
	return atomic.LoadInt32(&self.length)
}

func (self *ConcurrentSkipList) Level() int {
	return MAX_LEVEL
}

func (self *ConcurrentSkipList) randomLevel() int {
	level := 1
	kBranching := uint32(4)
	for (self.rng.Next()%kBranching == 0) && level < MAX_LEVEL {
		level++
	}
	return level

}

func search_helper(key KeyFace, current *node, preds []*node, succs []*node) int { //搜索辅助
	depth := MAX_LEVEL - 1
	//j := 0
	found := -1
	var pred, curr *node
	pred = current
	for i := depth; i >= 0; i-- {
		curr = pred.forward[i]
		for curr != nil {
			if curr.key != nil { //head和tail的key为nil
				//if curr.key.HashCode() < key.HashCode() {
				if curr.key.Less(key) {
					pred = curr
					curr = pred.forward[i]
					//j++
				} else {
					break
				}
			} else {
				break
			}
		}
		if preds != nil {
			preds[i] = pred
		}
		if succs != nil {
			succs[i] = curr
		}
		if found == -1 {
			if curr.key != nil {
				//if curr.key.HashCode() == key.HashCode() {
				if curr.key.Equals(key) {
					found = i
				}
			}
		}
	}
	//log.Println("find times is : ", j)
	return found
}

func (self *ConcurrentSkipList) Get(key KeyFace) (value interface{}, err error) {
	//succs := make([]*node, MAX_LEVEL)
	var succs [MAX_LEVEL]*node
	found := search_helper(key, self.header, nil, succs[:])
	if found != -1 {
		pnode_curr := succs[found]
		if pnode_curr != nil {
			if pnode_curr.key != nil {
				value = pnode_curr.value
			} else {
				err = errors.New("no element in the list!")
			}
		} else {
			panic("get a nil pointer!")
		}
	} else {
		err = errors.New("cant find element in the list!")
	}
	return
}

func (self *ConcurrentSkipList) Put(key KeyFace, value interface{}) (old_value interface{}, err error) {
	if key == nil || value == nil {
		err = errors.New("key or value is nil!")
		return
	}

	//preds := make([]*node, MAX_LEVEL)
	//succs := make([]*node, MAX_LEVEL)
	var preds [MAX_LEVEL]*node
	var succs [MAX_LEVEL]*node
	newLevel := self.randomLevel()
	waittimedelta := time.Duration(1) //sleep时间

	for {

		found := search_helper(key, self.header, preds[:], succs[:])
		if found != -1 { //之前已经插入过，更新之前的值即可
			pnode_curr := succs[found]
			if atomic.LoadInt32(&pnode_curr.deleted) != 1 {
				for atomic.LoadInt32(&pnode_curr.fullyLinked) != 1 {
					log.Println("wait fullyLinked!")
					runtime.Gosched()
				}
				if pnode_curr.key != nil {
					old_value = pnode_curr.value
					pnode_curr.value = value
				}
				return
			}
			time.Sleep(waittimedelta * time.Millisecond)
			waittimedelta <<= 1
			continue
		}

		node_set := make(map[*node]bool)
		lockok := true
		for i := 0; i < newLevel; i++ {
			pred := preds[i]
			succ := succs[i]
			v := node_set[pred]
			if !v {
				locked := pred.m.TryLock() //锁住前继
				if locked {
					node_set[pred] = true
				} else {
					lockok = false
					break
				}
			}

			if (atomic.LoadInt32(&pred.deleted) == 1) || (pred.forward[i] != succ) {
				lockok = false
				break
			}
			if pred.forward[i].key != nil {
				//if pred.forward[i].key.HashCode() == key.HashCode() {
				if pred.forward[i].key.Equals(key) {
					lockok = false
					break
				}
			}
		}

		if lockok == false {
			for k, _ := range node_set {
				k.m.Unlock()
			}
			time.Sleep(waittimedelta * time.Millisecond)
			waittimedelta <<= 1 //争抢激烈需要休眠更长时间
			continue
		}

		newNode := new(node)
		newNode.forward = make([]*node, newLevel)
		newNode.key = key
		newNode.value = value
		newNode.level = newLevel

		for i := 0; i < newLevel; i++ {
			newNode.forward[i] = preds[i].forward[i]
			preds[i].forward[i] = newNode
		}
		newNode.prevnode = preds[0]
		succs[0].prevnode = newNode

		atomic.StoreInt32(&newNode.fullyLinked, 1) //关系链设置完成

		for k, _ := range node_set {
			k.m.Unlock()
		}

		atomic.AddInt32(&self.length, int32(1))

		break

	}

	return
}

func (self *ConcurrentSkipList) UnsafePut(key KeyFace, value interface{}) (old_value interface{}, err error) {
	if key == nil || value == nil {
		err = errors.New("key or value is nil!")
		return
	}

	//preds := make([]*node, MAX_LEVEL)
	//succs := make([]*node, MAX_LEVEL)
	var preds [MAX_LEVEL]*node
	var succs [MAX_LEVEL]*node
	newLevel := self.randomLevel()

	found := search_helper(key, self.header, preds[:], succs[:])
	if found != -1 { //之前已经插入过，更新之前的值即可
		pnode_curr := succs[found]
		if pnode_curr.key != nil {
			old_value = pnode_curr.value
			pnode_curr.value = value
		}
		return
	}

	newNode := new(node)
	newNode.forward = make([]*node, newLevel)
	newNode.key = key
	newNode.value = value
	newNode.level = newLevel

	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = preds[i].forward[i]
		preds[i].forward[i] = newNode
	}
	newNode.prevnode = preds[0]
	succs[0].prevnode = newNode

	self.length += 1

	return
}

func (self *ConcurrentSkipList) Remove(key KeyFace) (value interface{}, err error) {
	if key == nil {
		err = errors.New("key is nil!")
		return
	}

	//preds := make([]*node, MAX_LEVEL)
	//succs := make([]*node, MAX_LEVEL)
	var preds [MAX_LEVEL]*node
	var succs [MAX_LEVEL]*node
	waittimedelta := time.Duration(1)

	for {

		found := search_helper(key, self.header, preds[:], succs[:])

		if found == -1 {
			err = errors.New("cant find the element!")
			return
		}

		pnode_curr := succs[found]
		if pnode_curr.key != nil {
			value = pnode_curr.value
		} else {
			err = errors.New("cant match the element!")
			return
		}

		locked := pnode_curr.m.TryLock()
		if locked {
			if atomic.LoadInt32(&pnode_curr.deleted) == 1 {
				pnode_curr.m.Unlock()
				return
			}
		} else {
			time.Sleep(waittimedelta * time.Millisecond)
			waittimedelta <<= 1
			continue
		}

		node_set := make(map[*node]bool)
		lockok := true
		node_set[pnode_curr] = true
		for i := 0; i < pnode_curr.Level(); i++ {
			pred := preds[i]
			succ := succs[i]
			v := node_set[pred]
			if !v {
				locked := pred.m.TryLock()
				if locked {
					node_set[pred] = true
				} else {
					lockok = false
					break
				}
			}
			if (atomic.LoadInt32(&pred.deleted) == 1) || (atomic.LoadInt32(&pred.fullyLinked) == 0) {
				lockok = false
				break
			}
			if (pred.forward[i] != pnode_curr) || (pred.forward[i] != succ) {
				lockok = false
				break
			}
		}

		if lockok == false {
			for k, _ := range node_set {
				k.m.Unlock()
			}
			time.Sleep(waittimedelta * time.Millisecond)
			waittimedelta <<= 1
			continue
		}

		for i := 0; i < pnode_curr.Level() && preds[i].forward[i] == pnode_curr; i++ {
			preds[i].forward[i] = pnode_curr.forward[i]
		}
		pnode_curr.forward[0].prevnode = preds[0]

		atomic.StoreInt32(&pnode_curr.deleted, 1)

		for k, _ := range node_set {
			k.m.Unlock()
		}

		atomic.AddInt32(&self.length, int32(-1))

		break

	}

	return

}

func (self *ConcurrentSkipList) UnsafeRemove(key KeyFace) (value interface{}, err error) {
	if key == nil {
		err = errors.New("key is nil!")
		return
	}

	//preds := make([]*node, MAX_LEVEL)
	//succs := make([]*node, MAX_LEVEL)
	var preds [MAX_LEVEL]*node
	var succs [MAX_LEVEL]*node

	found := search_helper(key, self.header, preds[:], succs[:])

	if found == -1 {
		err = errors.New("cant find the element!")
		return
	}

	pnode_curr := succs[found]
	if pnode_curr.key != nil {
		value = pnode_curr.value
	} else {
		err = errors.New("cant match the element!")
		return
	}

	for i := 0; i < pnode_curr.Level() && preds[i].forward[i] == pnode_curr; i++ {
		preds[i].forward[i] = pnode_curr.forward[i]
	}
	pnode_curr.forward[0].prevnode = preds[0]

	self.length -= 1

	return
}

func (self *ConcurrentSkipList) GetPrev(key KeyFace) (value interface{}, err error) {
	//succs := make([]*node, MAX_LEVEL)
	var preds [MAX_LEVEL]*node
	var succs [MAX_LEVEL]*node
	found := search_helper(key, self.header, preds[:], succs[:])
	if found != -1 {
		pnode_curr := preds[0]
		if pnode_curr != nil {
			if pnode_curr.key != nil {
				value = pnode_curr.value
			} else {
				err = errors.New("no element in the list!")
			}
		} else {
			panic("get a nil pointer!")
		}
	} else {
		err = errors.New("cant find element in the list!")
	}
	return
}

func (self *ConcurrentSkipList) GetNext(key KeyFace) (value interface{}, err error) {
	//succs := make([]*node, MAX_LEVEL)
	var succs [MAX_LEVEL]*node
	found := search_helper(key, self.header, nil, succs[:])
	if found != -1 {
		pnode_curr := succs[0]
		if pnode_curr != nil {
			if pnode_curr.key != nil {
				value = pnode_curr.value
			} else {
				err = errors.New("no element in the list!")
			}
		} else {
			panic("get a nil pointer!")
		}
	} else {
		err = errors.New("cant find element in the list!")
	}
	return
}

func (self *ConcurrentSkipList) Clear() {
	for i := 0; i < len(self.header.forward); i++ {
		self.header.forward[i] = self.tailer
	}
	atomic.StoreInt32(&self.header.fullyLinked, 1)
	atomic.StoreInt32(&self.tailer.fullyLinked, 1)
	self.length = 0
}

func (self *ConcurrentSkipList) GetFirst() (entry *Entry) {
	next := self.header.next()
	if next.key != nil {
		entry = new(Entry)
		entry.Key = next.key
		entry.Value = next.value
	}
	return
}

func (self *ConcurrentSkipList) GetLast() (entry *Entry) {
	prev := self.tailer.prev()
	if prev != nil {
		if prev != self.header {
			if prev.key != nil {
				entry = new(Entry)
				entry.Key = prev.key
				entry.Value = prev.value
			}
		}
	}
	return
}

////////////////////////////////////////////////////////////////////////////////

type ConnSkipListIterator struct { //迭代器
	slist  *ConcurrentSkipList
	curr   *node
	header *node
	tailer *node
}

func (self *ConcurrentSkipList) NewConnSkipListIterator() Iterator {
	ret := new(ConnSkipListIterator)
	ret.slist = self
	ret.curr = self.header
	ret.header = self.header
	ret.tailer = self.tailer
	return ret
}

func (self *ConnSkipListIterator) HasNext() bool {
	next := self.curr.next()
	if next == self.header || next == self.tailer {
		return false
	}
	if next != nil && next.key != nil {
		return true
	}
	return false
}

func (self *ConnSkipListIterator) Next() (entry *Entry) {
	next := self.curr.next()
	if next != nil && next.key != nil {
		entry = new(Entry)
		entry.Key = next.key
		entry.Value = next.value
		self.curr = next
		return
	}
	return
}
