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
		t.Fatalf("Expected `%+v`, got `%+v", e, a)
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
