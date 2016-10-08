package concurrent

type Entry struct {
	Key   KeyFace
	Value interface{}
}

type Iterator interface {
	HasNext() bool
	Next() *Entry
}
