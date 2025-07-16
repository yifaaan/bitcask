package main

import (
	"fmt"

	"github.com/yifaaan/bitcask"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "/tmp/bitcask-db"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("key1"), []byte("1"))
	if err != nil {
		panic(err)
	}

	value, err := db.Get([]byte("key1"))
	if err != nil {
		panic(err)
	}

	fmt.Println("value: ", string(value))

	err = db.Delete([]byte("key1"))
	if err != nil {
		panic(err)
	}

	_, err = db.Get([]byte("key1"))
	fmt.Println(err == bitcask.ErrKeyNotFound)
}
