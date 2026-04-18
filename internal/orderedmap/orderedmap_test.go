package orderedmap

import (
	"encoding/json"
	"testing"
)

func TestInsertionOrderPreserved(t *testing.T) {
	m := New[int]()
	m.Set("c", 3)
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("a", 10) // updating must not reorder
	got := m.Keys()
	want := []string{"c", "a", "b"}
	if len(got) != len(want) {
		t.Fatalf("keys mismatch: %v vs %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("key[%d]=%q want %q", i, got[i], want[i])
		}
	}
	if v, _ := m.Get("a"); v != 10 {
		t.Fatalf("expected updated value 10, got %d", v)
	}
}

func TestMarshalJSONPreservesOrder(t *testing.T) {
	m := New[int]()
	m.Set("z", 1)
	m.Set("a", 2)
	m.Set("m", 3)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"z":1,"a":2,"m":3}`
	if string(out) != want {
		t.Fatalf("marshal got %s want %s", string(out), want)
	}
}

func TestUnmarshalJSONRecordsSourceOrder(t *testing.T) {
	m := New[int]()
	if err := json.Unmarshal([]byte(`{"x":1,"b":2,"alpha":3}`), m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	keys := m.Keys()
	want := []string{"x", "b", "alpha"}
	for i, k := range want {
		if keys[i] != k {
			t.Fatalf("key[%d]=%q want %q", i, keys[i], k)
		}
	}
}

func TestDelete(t *testing.T) {
	m := New[int]()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)
	m.Delete("b")
	if m.Has("b") {
		t.Fatalf("b should be gone")
	}
	keys := m.Keys()
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "c" {
		t.Fatalf("unexpected keys after delete: %v", keys)
	}
}

func TestUpsert(t *testing.T) {
	m := New[int]()
	m.Upsert("n", func(cur int) int { return cur + 5 })
	m.Upsert("n", func(cur int) int { return cur + 7 })
	if v, _ := m.Get("n"); v != 12 {
		t.Fatalf("expected accumulator 12, got %d", v)
	}
}
