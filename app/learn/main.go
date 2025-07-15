package main

import "fmt"

func modMap(m map[int]string) {
	m[2] = "hello"
	m[3] = "goodbye"
	delete(m, 1)
}

func modSlice(s []int) {
	for k, v := range s {
		s[k] = v * 2
	}
	fmt.Println(s, len(s), cap(s))
	s = append(s, 10) // 仅修改了内部副本的长度
	fmt.Println(s, len(s), cap(s))
}

func main() {
	m := map[int]string{1: "first", 2: "second"}
	modMap(m)
	fmt.Println(m) // map[2:hello 3:goodbye]

	s := []int{1, 2, 3}
	modSlice(s)
	fmt.Println(s) // [2 4 6]
}
