package ffmap

import (
	"bytes"
	"iter"
	"os"
	"strconv"
	"strings"
	"testing"
)

const intRecordCount = 100
const stringRecordCount = 100
const structRecordCount = 100
const mapRecordCount = 100

func addDefaultRecords(m MutableFFMap) {
	for i := 1; i < intRecordCount; i++ {
		if err := m.Set("int:"+strconv.Itoa(i), i); err != nil {
			panic(err)
		}
	}
	for i := 1; i < stringRecordCount; i++ {
		str := "str:" + strconv.Itoa(i)
		if err := m.Set(str, str); err != nil {
			panic(err)
		}
	}
	for i := 1; i < structRecordCount; i++ {
		err := m.Set("TestNamedStruct:"+strconv.Itoa(i), TestNamedStruct{
			Value: "foo",
			ID:    123,
			Map:   map[string]TestNamedStruct{"bar": {Value: "bar", ID: 456, Bool: true}},
		})
		if err != nil {
			panic(err)
		}
	}
	for i := 1; i < mapRecordCount; i++ {
		mapValue := make(map[string]string)
		mapValue["foo"] = "bar"
		mapValue["bar"] = "foo"
		mapValue["foobar"] = ""
		if err := m.Set("map:"+strconv.Itoa(i), mapValue); err != nil {
			panic(err)
		}
	}
}

func BenchmarkCSVLoad(b *testing.B) {
	tmpFile, mOrig := makeTestMap(nil)
	defer os.Remove(tmpFile)
	addDefaultRecords(mOrig)
	var byteWriter bytes.Buffer
	if err := mOrig.commitTo(&byteWriter); err != nil {
		panic(err)
	}
	recordBytes := byteWriter.Bytes()

	for i := 0; i < b.N; i++ {
		bytesReader := bytes.NewReader(recordBytes)
		err := mOrig.loadFromReader(bytesReader)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkCSVDeleteAllSet(b *testing.B) {
	tmpFile, mOrig := makeTestMap(nil)
	defer os.Remove(tmpFile)

	for i := 0; i < b.N; i++ {
		mOrig.DeleteAll()
		addDefaultRecords(mOrig)
	}
}

func BenchmarkCSVGet(b *testing.B) {
	tmpFile, mOrig := makeTestMap(nil)
	defer os.Remove(tmpFile)
	addDefaultRecords(mOrig)

	for i := 0; i < b.N; i++ {
		for _, key := range mOrig.KeySet() {
			var err error
			if strings.HasPrefix(key, "int") {
				var v int
				_, err = mOrig.Get(key, &v)
			} else if strings.HasPrefix(key, "str") {
				var str string
				_, err = mOrig.Get(key, &str)
			} else if strings.HasPrefix(key, "TestNamedStruct") {
				var tns TestNamedStruct
				_, err = mOrig.Get(key, &tns)
			} else if strings.HasPrefix(key, "map") {
				var m map[string]string
				_, err = mOrig.Get(key, &m)
			}
			if err != nil {
				panic(err)
			}
		}
	}
}

type noOpWriter struct {
}

func (w *noOpWriter) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func BenchmarkCSVCommit(b *testing.B) {
	tmpFile, mOrig := makeTestMap(nil)
	defer os.Remove(tmpFile)
	addDefaultRecords(mOrig)
	writer := &noOpWriter{}

	for i := 0; i < b.N; i++ {
		mOrig.memoryMap.modCount++
		if err := mOrig.commitTo(writer); err != nil {
			panic(err)
		}
	}
}

// benchmarkTypedMap is a test wrapper that implements both iterator and callback approaches
type benchmarkTypedMap[T any] struct {
	ffm MutableFFMap
}

// All returns an iterator over all key-value pairs where values can be represented as type T
func (btm *benchmarkTypedMap[T]) All() iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		for _, key := range btm.ffm.KeySet() {
			var val T
			if ok, err := btm.ffm.Get(key, &val); ok && err == nil {
				if !yield(key, val) {
					return
				}
			}
		}
	}
}

// Range calls the provided function for each key-value pair where values can be represented as type T
func (btm *benchmarkTypedMap[T]) Range(fn func(string, T) bool) {
	for _, key := range btm.ffm.KeySet() {
		var val T
		if ok, err := btm.ffm.Get(key, &val); ok && err == nil {
			if !fn(key, val) {
				return
			}
		}
	}
}

func BenchmarkIteratorApproach(b *testing.B) {
	mOrig := NewMemoryMap()
	addDefaultRecords(mOrig)

	typedMap := &benchmarkTypedMap[int]{ffm: mOrig}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var count int
		for key, value := range typedMap.All() {
			count++
			_ = key
			_ = value
		}
	}
}

func BenchmarkCallbackApproach(b *testing.B) {
	mOrig := NewMemoryMap()
	addDefaultRecords(mOrig)

	typedMap := &benchmarkTypedMap[int]{ffm: mOrig}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var count int
		typedMap.Range(func(key string, value int) bool {
			count++
			_ = key
			_ = value
			return true
		})
	}
}
