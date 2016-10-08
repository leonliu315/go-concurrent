package concurrent

import (
	//"log"
	//"time"
	//"bytes"
	//"encoding/binary"
	"runtime"
	"sync/atomic"
	//"unsafe"
)

type KeyFace interface {
	HashCode() uint64
	Equals(o interface{}) bool
	Less(o interface{}) bool
	//More(o interface{}) bool
}

///////////////////////////////////////////////////////////////////////////////
//strkey
const (
	c1 uint32 = 0xcc9e2d51
	c2 uint32 = 0x1b873593
)

func Gen_ID(str string) int32 {
	return int32(MurmurHash3_32([]byte(str), 0x9747b28c))
}

func MurmurHash3_32(key []byte, seed uint32) uint32 { //seed 0x9747b28c

	h1 := seed
	slen := uint32(len(key))

	roundedEnd := slen & 0xfffffffc

	for i := uint32(0); i < roundedEnd; i += 4 {
		// little endian load order
		k1 := uint32(key[i])&0xff | uint32((key[i+1]))&0xff<<8 | uint32((key[i+2]))&0xff<<16 | uint32(key[i+3])&0xff<<24
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19)
		h1 = h1*5 + 0xe6546b64
	}

	var k1 uint32 = 0
	lastlen := slen & 0x03
	if lastlen >= 3 {
		k1 = uint32(key[roundedEnd+2]) & 0xff << 16
	}
	if lastlen >= 2 {
		k1 |= uint32(key[roundedEnd+1]) & 0xff << 8
	}
	if lastlen >= 1 {
		k1 |= uint32(key[roundedEnd]) & 0xff
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17) // ROTL32(k1,15);
		k1 *= c2
		h1 ^= k1
	}

	// finalization
	h1 ^= slen

	// fmix(h1);
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}

func BKDRHash(str string) int32 { //for string hash

	var seed int32 = int32(13131) // 31 131 1313 13131 131313 etc..
	var h int32 = 0

	strbytes := []byte(str)

	for i := 0; i < len(str); i++ {
		h = h*seed + int32(strbytes[i])
	}

	return h
}

type StrKey struct {
	hashv  uint32
	keystr string
}

func NewStrKey(k string) *StrKey {
	ret := new(StrKey)
	ret.Init(k)
	return ret
}

func (self *StrKey) Init(k string) {
	self.hashv = MurmurHash3_32([]byte(k), 0x9747b28c)
	self.keystr = k
}

func (self *StrKey) HashCode() uint64 {
	return uint64(self.hashv)
}

func (self *StrKey) Equals(o interface{}) bool {
	tmpStrKey := o.(*StrKey)
	//fmt.Println("Equals")
	return self.keystr == tmpStrKey.keystr
}

func (self *StrKey) Less(o interface{}) bool {
	tmpStrKey := o.(*StrKey)
	return self.hashv < tmpStrKey.hashv
}

func (self *StrKey) GetValue() string {
	return self.keystr
}

//int32key
type Int32Key struct {
	hashv int32
}

func NewInt32Key(k int32) *Int32Key {
	ret := new(Int32Key)
	ret.Init(k)
	return ret
}

func (self *Int32Key) Init(k int32) {
	self.hashv = k
}

func (self *Int32Key) HashCode() uint64 {
	return uint64(self.hashv)
}

func (self *Int32Key) Equals(o interface{}) bool {
	tmpInt32Key := o.(*Int32Key)
	return self.hashv == tmpInt32Key.hashv
}

func (self *Int32Key) Less(o interface{}) bool {
	tmpInt32Key := o.(*Int32Key)
	return self.hashv < tmpInt32Key.hashv
}

func (self *Int32Key) GetValue() int32 {
	return self.hashv
}

//int64key
type Int64Key struct {
	hashv int64
}

func NewInt64Key(k int64) *Int64Key {
	ret := new(Int64Key)
	ret.Init(k)
	return ret
}

func (self *Int64Key) Init(k int64) {
	self.hashv = k
}

func (self *Int64Key) HashCode() uint64 {
	return uint64(self.hashv)
}

func (self *Int64Key) Equals(o interface{}) bool {
	tmpInt64Key := o.(*Int64Key)
	return self.hashv == tmpInt64Key.hashv
}

func (self *Int64Key) Less(o interface{}) bool {
	tmpInt64Key := o.(*Int64Key)
	return self.hashv < tmpInt64Key.hashv
}

func (self *Int64Key) GetValue() int64 {
	return self.hashv
}

//str64key  低碰撞率
func MurmurHash64A(key []byte, seed uint64) uint64 { //seed 0xe17a1465
	m := uint64(0xc6a4a7935bd1e995)
	r := uint64(47)

	slen := uint64(len(key))
	h := seed ^ (slen * m)

	var k uint64

	flen := slen / 8

	seek := uint64(0)
	for i := uint64(0); i < flen; i++ {
		seek = i * 8
		//binary.Read(bytes.NewReader(key[seek:]), binary.LittleEndian, &k)
		k = uint64(key[seek+0])&0xff + uint64(key[seek+1])&0xff<<8 + uint64(key[seek+2])&0xff<<16 + uint64(key[seek+3])&0xff<<24 + uint64(key[seek+4])&0xff<<32 + uint64(key[seek+5])&0xff<<40 + uint64(key[seek+6])&0xff<<48 + uint64(key[seek+7])&0xff<<56
		k *= m
		k ^= k >> r
		k *= m

		h ^= k
		h *= m
	}

	elen := slen & 7
	data2 := key[slen-elen:]

	if elen >= 7 {
		h ^= uint64(data2[6]) & 0xff << 48
	}
	if elen >= 6 {
		h ^= uint64(data2[5]) & 0xff << 40
	}
	if elen >= 5 {
		h ^= uint64(data2[4]) & 0xff << 32
	}
	if elen >= 4 {
		h ^= uint64(data2[3]) & 0xff << 24
	}
	if elen >= 3 {
		h ^= uint64(data2[2]) & 0xff << 16
	}
	if elen >= 2 {
		h ^= uint64(data2[1]) & 0xff << 8
	}
	if elen >= 1 {
		h ^= uint64(data2[0]) & 0xff
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r

	return h
}

type Str64Key struct {
	hashv  uint64
	keystr string
}

func NewStr64Key(key string) *Str64Key {
	ret := new(Str64Key)
	ret.Init(key)
	return ret
}

func (self *Str64Key) Init(key string) {
	self.hashv = MurmurHash64A([]byte(key), 0xe17a1465)
	self.keystr = key
}

func (self *Str64Key) HashCode() uint64 {
	return self.hashv
}

func (self *Str64Key) Equals(o interface{}) bool {
	tmpStr64Key := o.(*Str64Key)
	return self.keystr == tmpStr64Key.keystr
}

func (self *Str64Key) Less(o interface{}) bool {
	tmpStr64Key := o.(*Str64Key)
	return self.hashv < tmpStr64Key.hashv
}

func (self *Str64Key) GetValue() string {
	return self.keystr
}

//float32key
type Float32Key struct {
	hashv float32
}

func NewFloat32Key(k float32) *Float32Key {
	ret := new(Float32Key)
	ret.Init(k)
	return ret
}

func (self *Float32Key) Init(k float32) {
	self.hashv = k
}

func (self *Float32Key) HashCode() uint64 {
	return uint64(self.hashv)
}

func (self *Float32Key) Equals(o interface{}) bool {
	tmpFloat32Key := o.(*Float32Key)
	return self.hashv == tmpFloat32Key.hashv
}

func (self *Float32Key) Less(o interface{}) bool {
	tmpFloat32Key := o.(*Float32Key)
	return self.hashv < tmpFloat32Key.hashv
}

func (self *Float32Key) GetValue() float32 {
	return self.hashv
}

////////////////////////////////////////////////////////////////////////////////

//type Mutex struct {
//	c chan struct{}
//}

//func NewMutex() *Mutex {
//	return &Mutex{make(chan struct{}, 1)}
//}

//func (m *Mutex) Lock() {
//	m.c <- struct{}{}
//}

//func (m *Mutex) Unlock() {
//	<-m.c
//}

//func (m *Mutex) TryLock(timeout time.Duration) bool {
//	//timer := time.NewTimer(timeout)
//	select {
//	case m.c <- struct{}{}:
//		//timer.Stop()
//		return true
//	case <-time.After(timeout):
//	}
//	return false
//}

const (
	locked   int32 = 1
	unlocked int32 = 0

	max_spin int32 = 16
)

// spinlock
type Mutex struct {
	lock int32
}

func (m *Mutex) Lock() {
	spin := int32(0)
	for {
		if m.TryLock() {
			return
		}
		spin++

		if spin >= max_spin {
			runtime.Gosched()
			spin = 0
		}
	}
}

func (m *Mutex) MeanLock() {
	spin := int32(0)
	for {
		if m.TryLock() {
			return
		}
		spin++

		if spin >= int32(1<<18) {
			runtime.Gosched()
			spin = 0
		}
	}
}

func (m *Mutex) TryLock() bool {
	return atomic.CompareAndSwapInt32(&m.lock, unlocked, locked)
}

func (m *Mutex) Unlock() {
	atomic.StoreInt32(&m.lock, unlocked)
}
