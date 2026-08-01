package bitcask

import (
	"slices"
	"testing"
)

type iteratorTestEntry struct {
	key   string
	value string
}

func openIteratorTestDB(t *testing.T) (*DB, []iteratorTestEntry) {
	t.Helper()

	entries := []iteratorTestEntry{
		{key: "a-1", value: "value-a1"},
		{key: "a-2", value: "value-a2"},
		{key: "b-1", value: "value-b1"},
		{key: "b-2", value: "value-b2"},
		{key: "c-1", value: "value-c1"},
		{key: "c-2", value: "value-c2"},
	}

	db, _ := openTestDB(t)
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if err := db.Put([]byte(entry.key), []byte(entry.value)); err != nil {
			t.Fatalf("Put(%q) error = %v", entry.key, err)
		}
	}
	return db, entries
}

func collectIteratorEntries(t *testing.T, it *Iterator) []iteratorTestEntry {
	t.Helper()

	var got []iteratorTestEntry
	for it.Valid() {
		value, err := it.Value()
		if err != nil {
			t.Fatalf("Iterator.Value() error = %v", err)
		}
		got = append(got, iteratorTestEntry{
			key:   string(it.Key()),
			value: string(value),
		})
		it.Next()
	}
	return got
}

func assertIteratorEntries(t *testing.T, got, want []iteratorTestEntry) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("iterator entries = %#v, want %#v", got, want)
	}
}

func TestIteratorIterationOrder(t *testing.T) {
	db, entries := openIteratorTestDB(t)
	defer closeTestDB(t, db)

	tests := []struct {
		name    string
		reverse bool
		want    []iteratorTestEntry
	}{
		{
			name: "forward",
			want: entries,
		},
		{
			name:    "reverse",
			reverse: true,
			want:    func() []iteratorTestEntry { result := slices.Clone(entries); slices.Reverse(result); return result }(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := db.NewIterator(IteratorOptions{Reverse: tt.reverse})
			defer it.Close()

			it.Rewind()
			assertIteratorEntries(t, collectIteratorEntries(t, it), tt.want)
		})
	}
}

func TestIteratorPrefix(t *testing.T) {
	db, _ := openIteratorTestDB(t)
	defer closeTestDB(t, db)

	tests := []struct {
		name    string
		reverse bool
		want    []iteratorTestEntry
	}{
		{
			name: "forward",
			want: []iteratorTestEntry{
				{key: "b-1", value: "value-b1"},
				{key: "b-2", value: "value-b2"},
			},
		},
		{
			name:    "reverse",
			reverse: true,
			want: []iteratorTestEntry{
				{key: "b-2", value: "value-b2"},
				{key: "b-1", value: "value-b1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := db.NewIterator(IteratorOptions{
				Prefix:  []byte("b-"),
				Reverse: tt.reverse,
			})
			defer it.Close()

			it.Rewind()
			assertIteratorEntries(t, collectIteratorEntries(t, it), tt.want)
		})
	}
}

func TestIteratorSeek(t *testing.T) {
	db, entries := openIteratorTestDB(t)
	defer closeTestDB(t, db)

	reverseEntries := slices.Clone(entries)
	slices.Reverse(reverseEntries)

	tests := []struct {
		name    string
		options IteratorOptions
		seek    string
		want    []iteratorTestEntry
	}{
		{
			name: "forward exact",
			seek: "b-1",
			want: entries[2:],
		},
		{
			name: "forward insertion point",
			seek: "b-0",
			want: entries[2:],
		},
		{
			name: "forward after last",
			seek: "z",
			want: nil,
		},
		{
			name:    "reverse exact",
			options: IteratorOptions{Reverse: true},
			seek:    "b-1",
			want:    reverseEntries[3:],
		},
		{
			name:    "reverse insertion point",
			options: IteratorOptions{Reverse: true},
			seek:    "b-0",
			want:    reverseEntries[4:],
		},
		{
			name:    "reverse before first",
			options: IteratorOptions{Reverse: true},
			seek:    "0",
			want:    nil,
		},
		{
			name: "forward prefix seek",
			options: IteratorOptions{
				Prefix: []byte("b-"),
			},
			seek: "a-2",
			want: entries[2:4],
		},
		{
			name: "reverse prefix seek",
			options: IteratorOptions{
				Prefix:  []byte("b-"),
				Reverse: true,
			},
			seek: "c-1",
			want: []iteratorTestEntry{
				{key: "b-2", value: "value-b2"},
				{key: "b-1", value: "value-b1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := db.NewIterator(tt.options)
			defer it.Close()

			it.Seek([]byte(tt.seek))
			assertIteratorEntries(t, collectIteratorEntries(t, it), tt.want)
		})
	}
}

func TestIteratorClose(t *testing.T) {
	db, _ := openIteratorTestDB(t)
	defer closeTestDB(t, db)

	it := db.NewIterator(DefaultIteratorOptions)
	it.Close()

	if it.Valid() {
		t.Fatal("Iterator.Valid() = true after Close(), want false")
	}
}
