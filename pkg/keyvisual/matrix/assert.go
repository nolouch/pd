package matrix

import (
	"reflect"
	"testing"
)

func AssertEqual(t *testing.T, e, a interface{}) {
	if (e == nil || a == nil) && (isNil(e) && isNil(a)) {
		return
	}
	if !reflect.DeepEqual(e, a) {
		t.Fatalf("Expected `%+v`, got `%+v`", e, a)
	}
}

func AssertIsNil(t *testing.T, v interface{}) {
	AssertEqual(t, nil, v)
}

func AssertNotNil(t *testing.T, v interface{}) {
	if v == nil {
		t.Fatalf("expected non-nil, got %+v", v)
	}
}

func AssertTrue(t *testing.T, v bool) {
	AssertEqual(t, true, v)
}

func AssertFalse(t *testing.T, v bool) {
	AssertEqual(t, false, v)
}

func isNil(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() != reflect.Struct && rv.IsNil()
}

func PanicExists(t *testing.T) {
	e := recover()
	AssertNotNil(t, e)
}

func AssertTwoDimUint64ArrayAllEqual(t *testing.T, array [][]uint64, targetValue uint64) {
	for _, subArray := range array {
		for index := range subArray {
			AssertEqual(t, subArray[index], targetValue)
		}
	}
}

func AssertArrayEqual(t *testing.T, array1 []uint64, array2 []uint64) {
	AssertEqual(t, len(array1), len(array2))
	for idx := range array1 {
		AssertEqual(t, array1[idx], array2[idx])
	}
}
