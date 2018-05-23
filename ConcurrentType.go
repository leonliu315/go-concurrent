package concurrent

import (
	//"log"
	//"time"
	//"bytes"
	//"encoding/binary"
	"runtime"
	"strings"
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

/*  integer  hash

unsigned int hash(unsigned int x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

unsigned int unhash(unsigned int x) {
    x = ((x >> 16) ^ x) * 0x119de1f3;
    x = ((x >> 16) ^ x) * 0x119de1f3;
    x = (x >> 16) ^ x;
    return x;
}


uint64_t hash(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
}


uint64_t unhash(uint64_t x) {
    x = (x ^ (x >> 31) ^ (x >> 62)) * UINT64_C(0x319642b2d24d8ec3);
    x = (x ^ (x >> 27) ^ (x >> 54)) * UINT64_C(0x96de1b173f119089);
    x = x ^ (x >> 30) ^ (x >> 60);
    return x;
}

*/

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

type CStrKey struct {
	keystr string
}

func NewCStrKey(k string) *CStrKey {
	ret := new(CStrKey)
	ret.Init(k)
	return ret
}

func (self *CStrKey) Init(k string) {
	self.keystr = k
}

func (self *CStrKey) HashCode() uint64 {
	return uint64(MurmurHash3_32([]byte(self.keystr), 0x9747b28c))
}

func (self *CStrKey) Equals(o interface{}) bool {
	tmpStrKey := o.(*CStrKey)
	//fmt.Println("Equals")
	return strings.Compare(self.keystr, tmpStrKey.keystr) == 0
}

func (self *CStrKey) Less(o interface{}) bool {
	tmpStrKey := o.(*CStrKey)
	return strings.Compare(self.keystr, tmpStrKey.keystr) == -1
}

func (self *CStrKey) GetValue() string {
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

var crctab16 [256]uint16 = [256]uint16{
	0X0000, 0X1189, 0X2312, 0X329B, 0X4624, 0X57AD, 0X6536, 0X74BF,
	0X8C48, 0X9DC1, 0XAF5A, 0XBED3, 0XCA6C, 0XDBE5, 0XE97E, 0XF8F7,
	0X1081, 0X0108, 0X3393, 0X221A, 0X56A5, 0X472C, 0X75B7, 0X643E,
	0X9CC9, 0X8D40, 0XBFDB, 0XAE52, 0XDAED, 0XCB64, 0XF9FF, 0XE876,
	0X2102, 0X308B, 0X0210, 0X1399, 0X6726, 0X76AF, 0X4434, 0X55BD,
	0XAD4A, 0XBCC3, 0X8E58, 0X9FD1, 0XEB6E, 0XFAE7, 0XC87C, 0XD9F5,
	0X3183, 0X200A, 0X1291, 0X0318, 0X77A7, 0X662E, 0X54B5, 0X453C,
	0XBDCB, 0XAC42, 0X9ED9, 0X8F50, 0XFBEF, 0XEA66, 0XD8FD, 0XC974,
	0X4204, 0X538D, 0X6116, 0X709F, 0X0420, 0X15A9, 0X2732, 0X36BB,
	0XCE4C, 0XDFC5, 0XED5E, 0XFCD7, 0X8868, 0X99E1, 0XAB7A, 0XBAF3,
	0X5285, 0X430C, 0X7197, 0X601E, 0X14A1, 0X0528, 0X37B3, 0X263A,
	0XDECD, 0XCF44, 0XFDDF, 0XEC56, 0X98E9, 0X8960, 0XBBFB, 0XAA72,
	0X6306, 0X728F, 0X4014, 0X519D, 0X2522, 0X34AB, 0X0630, 0X17B9,
	0XEF4E, 0XFEC7, 0XCC5C, 0XDDD5, 0XA96A, 0XB8E3, 0X8A78, 0X9BF1,
	0X7387, 0X620E, 0X5095, 0X411C, 0X35A3, 0X242A, 0X16B1, 0X0738,
	0XFFCF, 0XEE46, 0XDCDD, 0XCD54, 0XB9EB, 0XA862, 0X9AF9, 0X8B70,
	0X8408, 0X9581, 0XA71A, 0XB693, 0XC22C, 0XD3A5, 0XE13E, 0XF0B7,
	0X0840, 0X19C9, 0X2B52, 0X3ADB, 0X4E64, 0X5FED, 0X6D76, 0X7CFF,
	0X9489, 0X8500, 0XB79B, 0XA612, 0XD2AD, 0XC324, 0XF1BF, 0XE036,
	0X18C1, 0X0948, 0X3BD3, 0X2A5A, 0X5EE5, 0X4F6C, 0X7DF7, 0X6C7E,
	0XA50A, 0XB483, 0X8618, 0X9791, 0XE32E, 0XF2A7, 0XC03C, 0XD1B5,
	0X2942, 0X38CB, 0X0A50, 0X1BD9, 0X6F66, 0X7EEF, 0X4C74, 0X5DFD,
	0XB58B, 0XA402, 0X9699, 0X8710, 0XF3AF, 0XE226, 0XD0BD, 0XC134,
	0X39C3, 0X284A, 0X1AD1, 0X0B58, 0X7FE7, 0X6E6E, 0X5CF5, 0X4D7C,
	0XC60C, 0XD785, 0XE51E, 0XF497, 0X8028, 0X91A1, 0XA33A, 0XB2B3,
	0X4A44, 0X5BCD, 0X6956, 0X78DF, 0X0C60, 0X1DE9, 0X2F72, 0X3EFB,
	0XD68D, 0XC704, 0XF59F, 0XE416, 0X90A9, 0X8120, 0XB3BB, 0XA232,
	0X5AC5, 0X4B4C, 0X79D7, 0X685E, 0X1CE1, 0X0D68, 0X3FF3, 0X2E7A,
	0XE70E, 0XF687, 0XC41C, 0XD595, 0XA12A, 0XB0A3, 0X8238, 0X93B1,
	0X6B46, 0X7ACF, 0X4854, 0X59DD, 0X2D62, 0X3CEB, 0X0E70, 0X1FF9,
	0XF78F, 0XE606, 0XD49D, 0XC514, 0XB1AB, 0XA022, 0X92B9, 0X8330,
	0X7BC7, 0X6A4E, 0X58D5, 0X495C, 0X3DE3, 0X2C6A, 0X1EF1, 0X0F78,
}

// 计算给定长度数据的 16 位 CRC。
func GetCrc16(pdata []byte) (fcs uint16) {
	fcs = 0xffff // 初始化
	for counti := range pdata {
		//		fmt.Printf("pdata[%d]=%x\n", counti, pdata[counti])
		fcs = (fcs >> 8) ^ crctab16[uint8(fcs^uint16(pdata[counti]))&0x00ff]
	}
	return ^fcs // 取反
}

var crc16tab = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

func crc16(buf string) uint16 {
	var crc uint16
	for _, n := range buf {
		crc = (crc << uint16(8)) ^ crc16tab[((crc>>uint16(8))^uint16(n))&0x00FF]
	}
	return crc
}

type Str16Key struct {
	hashv  uint16
	keystr string
}

func NewStr16Key(k string) *Str16Key {
	ret := new(Str16Key)
	ret.Init(k)
	return ret
}

func (self *Str16Key) Init(k string) {
	self.hashv = crc16(k)
	self.keystr = k
}

func (self *Str16Key) HashCode() uint64 {
	return uint64(self.hashv)
}

func (self *Str16Key) Equals(o interface{}) bool {
	tmpStrKey := o.(*Str16Key)
	//fmt.Println("Equals")
	return self.keystr == tmpStrKey.keystr
}

func (self *Str16Key) Less(o interface{}) bool {
	tmpStrKey := o.(*Str16Key)
	return self.hashv < tmpStrKey.hashv
}

func (self *Str16Key) GetValue() string {
	return self.keystr
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
