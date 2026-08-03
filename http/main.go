package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/yifaaan/bitcask"
)

var db *bitcask.DB

func init() {
	var err error
	options := bitcask.DefaultOptions
	options.DirPath = "./bitcask-db/"
	db, err = bitcask.Open(options)
	if err != nil {
		panic("failed  to open db")
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
	}

	for k, v := range data {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put value in db: %v\n", err)
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	k := request.URL.Query().Get("key")

	v, err := db.Get([]byte(k))
	if err != nil && err != bitcask.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value in db: %v\n", err)
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(v))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	k := request.URL.Query().Get("key")

	err := db.Delete([]byte(k))
	if err != nil && err != bitcask.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete value in db: %v\n", err)
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()

	var result []string
	for _, k := range keys {
		result = append(result, string(k))
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	st := db.Stat()

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(st)
}

func main() {
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)

	http.ListenAndServe("localhost:8080", nil)
}
