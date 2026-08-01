package main

import (
	"fmt"

	"github.com/yifaaan/bitcask"
)

func main() {
	opts := bitcask.DefaultOptions

	opts.DirPath = "D:/Code/bitcask"
	// defer os.RemoveAll(opts.DirPath)
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	val, _ := db.Get([]byte("name"))
	fmt.Printf("val=%s\n", val)
}
