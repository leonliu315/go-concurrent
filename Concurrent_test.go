package concurrent

import (
	"log"
	//"runtime"
	//"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {

	//runtime.GOMAXPROCS(runtime.NumCPU())

	testcskiplist = NewConcurrentSkipList()
	testhsmap = NewConcurrentHashMap(10000)

	for i := 0; i < 1000000; i++ {
		key := NewInt32Key(int32(i))
		testcskiplist.Put(key, i)
	}

}

var testlock sync.Mutex

var testcskiplist *ConcurrentSkipList
var testhsmap *ConcurrentHashMap

func TestStrKey(t *testing.T) {

	a := NewStr64Key("123456")
	b := NewStr64Key("1234567")
	log.Println("a hashcode:", int32(a.HashCode()))
	log.Println("b hashcode:", int32(b.HashCode()))
	log.Println("a==b:", a.Equals(b))
	log.Println("getvalue:", a.GetValue())

	//c := NewStrKey("d1as32g123as1dg23as1d23g")

	chash := NewConcurrentHashMap(100)
	chash.Put(b, 123456789)
	_, err := chash.Put(a, 1)
	if err != nil {
		log.Println("err is ", err.Error())
	}
	//var iter Iterator
	//iter = chash.NewConcurrentHashMapIterator()
	////iter.HasNext()
	//for iter.HasNext() {
	//	log.Println("the data:", iter.Next())
	//}
	//for iter.HasNext() {
	//	log.Println(iter.Next())
	//}
	//log.Println("v is ", v)
	//log.Println("num is ", chash.Size())
	//v, err = chash.Get(a)
	//if err != nil {
	//	log.Println("err is ", err.Error())
	//}
	//log.Println("v is ", v)
	//v, err = chash.Remove(a)
	//if err != nil {
	//	log.Println("err is ", err.Error())
	//}
	//log.Println("v is ", v)

}

func TestInt32Key(t *testing.T) {
	chash := NewConcurrentHashMap(100)
	testmap := make(map[int32]interface{})
	for i := 0; i < 500; i++ {
		a := NewInt32Key(int32(i))
		old, err := chash.Put(a, i*10)
		if err != nil {
			log.Println(err.Error())
		}
		if old != nil {
			log.Println(old)
		}
	}
	var iter Iterator
	iter = chash.NewConcurrentHashMapIterator()
	j := 0
	for iter.HasNext() {
		iter.Next()
		//log.Println("the data:", iter.Next())
		j++
	}
	c := NewInt32Key(int32(100))
	v, e := chash.Remove(c)
	log.Println(v, e)
	for i := 0; i < 500; i++ {
		a := NewInt32Key(int32(i))
		old, err := chash.Get(a)
		if err != nil {
			log.Println(err.Error())
		}
		if old == nil {
			log.Println(i)
		}
	}
	log.Println("trave num is ", j)
	log.Println("map size is : ", len(testmap))
	log.Println("chash size : ", chash.Size())
}

func Benchmark_Chash(b *testing.B) {
	a := NewInt32Key(int32(1234356))
	chash := NewConcurrentHashMap(100)
	_, err := chash.Put(a, 1)
	if err != nil {
		log.Println("err is ", err.Error())
	}

	for i := 0; i < b.N; i++ { //use b.N for looping
		tk := NewInt32Key(int32(i))
		chash.Get(tk)
		//chash.Put(a, 1)
	}
}

func Benchmark_hash(b *testing.B) {
	//chash := make(map[string]int)
	//chash := make(map[int32]int)
	//chash[1234356] = 1
	//var m sync.Mutex
	key := NewInt32Key(int32(623150))
	for i := 0; i < b.N; i++ { //use b.N for looping
		//m.Lock()
		//_ = chash[1234356]
		//m.Unlock()

		testcskiplist.Get(key)
	}
}

var teststr string = "90E79AE5-2905-452C-9CC7-D63090639D6E"

func Benchmark_hash_func1(b *testing.B) {
	for i := 0; i < b.N; i++ { //use b.N for looping
		BKDRHash(teststr)
	}
}

func Benchmark_hash_func2(b *testing.B) { //32bit<64bit cpu 差别大
	by := []byte(teststr)
	for i := 0; i < b.N; i++ { //use b.N for looping
		MurmurHash64A(by, 0xe17a1465)
	}
}

func Benchmark_hash_func3(b *testing.B) { //32bit<64bit cpu 差别大
	by := []byte(teststr)
	for i := 0; i < b.N; i++ { //use b.N for looping
		MurmurHash3_32(by, 0x9747b28c)
	}
}

func TestCskiplist(t *testing.T) {
	csk := NewConcurrentSkipList()
	c := NewInt32Key(int32(49900))
	csk.Get(c)
	for i := 0; i < 50000; i++ {
		a := NewInt32Key(int32(i))
		//a := NewStrKey(strconv.Itoa(i))
		csk.Put(a, i)
	}
	//v, e := csk.Remove(c)
	//log.Println(v, e)
	var iter Iterator
	iter = csk.NewConnSkipListIterator()
	for iter.HasNext() {
		entry := iter.Next()
		enkey := entry.Key
		csk.Remove(enkey)
		//log.Println(iter.Next().Key.HashCode())
	}
	vv, ee := csk.Get(c)
	if ee != nil {
		log.Println(ee.Error())
	}
	log.Println("last find : ", vv, " len:", csk.Len())
	//for i := 0; i < 500; i++ {
	//	a := NewInt32Key(int32(i))
	//	v, err := csk.Get(a)
	//	log.Println("test get", v, err)
	//}
}

var lscount int32

func testsklist1() {
	//time.Sleep(1 * time.Millisecond)
	for i := 0; i < 10000; i++ {
		n := i * 2
		key := NewInt32Key(int32(n))
		testcskiplist.Put(key, n)
		//old, err := testcskiplist.Put(key, n)
		//log.Println("testsklist1:", old, " ", err, " ovalue: ", n)
	}
	log.Println("testsklist1 ok")
	atomic.AddInt32(&lscount, int32(1))
}

func testsklist2() {
	//time.Sleep(1 * time.Millisecond)
	for i := 0; i < 10000; i++ {
		n := i*2 + 1
		key := NewInt32Key(int32(n))
		testcskiplist.Put(key, n)
		//old, err := testcskiplist.Put(key, n)
		//log.Println("testsklist2:", old, " ", err, " ovalue: ", n)
	}
	log.Println("testsklist2 ok")
	atomic.AddInt32(&lscount, int32(1))
}

func testsklist3() {
	time.Sleep(1 * time.Millisecond)
	j := 0
	for i := 1; i < 5000; i++ {
		key := NewInt32Key(int32(i))
		_, e := testcskiplist.Remove(key)
		if e != nil {
			//log.Println("cant remove : ", i, " err is : ", e.Error())
		} else {
			j++
		}
	}
	log.Println("remove num : ", j)
	atomic.AddInt32(&lscount, int32(1))
}

func TestCskiplistConcurrent(t *testing.T) {
	//for i := 0; i < 500; i++ {
	//	go testsklist2()
	//	go testsklist1()
	//	go testsklist3()
	//}
	////for i := 0; i < 5; i++ {
	////	go testsklist3()
	////}
	//time.Sleep(60 * time.Second)
	//log.Println("testcskiplist.Len() : ", testcskiplist.Len())
	//var iter Iterator
	//j := 0
	//iter = testcskiplist.NewConnSkipListIterator()
	//for iter.HasNext() {
	//	iter.Next()
	//	//log.Println(iter.Next())
	//	j++
	//	//if j == 5000 {
	//	//	break
	//	//}
	//}
	//key := NewInt32Key(int32(19789))
	//testcskiplist.Get(key)
	//log.Println("num is : ", j)
	//log.Println("lscount:", lscount)
}

func testcumap1() {
	//time.Sleep(1 * time.Millisecond)
	for i := 0; i < 10000; i++ {
		n := i * 2
		key := NewInt32Key(int32(n))
		testhsmap.Put(key, n)
		//old, err := testcskiplist.Put(key, n)
		//log.Println("testsklist1:", old, " ", err, " ovalue: ", n)
	}
	log.Println("testcumap1 ok")
}

func testcumap2() {
	//time.Sleep(1 * time.Millisecond)
	for i := 0; i < 10000; i++ {
		n := i*2 + 1
		key := NewInt32Key(int32(n))
		testhsmap.Put(key, n)
		//old, err := testcskiplist.Put(key, n)
		//log.Println("testsklist2:", old, " ", err, " ovalue: ", n)
	}
	log.Println("testcumap2 ok")
}

func testcumap3() {
	j := 0
	for i := 1; i < 5000; i++ {
		key := NewInt32Key(int32(i))
		_, e := testhsmap.Remove(key)
		if e != nil {
			//log.Println("cant remove : ", i, " err is : ", e.Error())
		} else {
			j++
		}
	}
	log.Println("remove num : ", j)
}

func TestCumapConcurrent(t *testing.T) {
	//for i := 0; i < 500; i++ {
	//	go testcumap2()
	//	go testcumap1()
	//	go testcumap3()
	//}
	//time.Sleep(60 * time.Second)
	//log.Println("testhsmap.Len() : ", testhsmap.Len())
	//var iter Iterator
	//j := 0
	//iter = testhsmap.NewConcurrentHashMapIterator()
	//for iter.HasNext() {
	//	iter.Next()
	//	//log.Println(iter.Next())
	//	j++
	//	//if j == 5000 {
	//	//	break
	//	//}
	//}
	//log.Println("num is : ", j)
	removetest := NewConcurrentHashMap(10)
	testkey := NewStr64Key("123456")
	removetest.Put(testkey, 1)
	removetest.Remove(testkey)

	k, _ := removetest.Get(testkey)
	log.Println("remove test:", k)

}

func Test_skiplistprev(t *testing.T) {
	skiplist := NewConcurrentSkipList()
	key := NewInt32Key(int32(1))
	skiplist.Put(key, int32(1))
	key = NewInt32Key(int32(2))
	skiplist.Put(key, int32(2))

	key = NewInt32Key(int32(2))
	skiplist.Remove(key)
	//	key = NewInt32Key(int32(1))
	//	skiplist.Remove(key)

	entry := skiplist.GetLast()
	log.Println("key, value : ", entry.Key, entry.Value)
}
