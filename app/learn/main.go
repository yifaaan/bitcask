package main

import (
	"fmt"
	"reflect"
)

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

func dumpMethodSet(i interface{}) {
	dynType := reflect.TypeOf(i)
	if dynType == nil {
		fmt.Println("There is no  dynamic type")
		return
	}
	n := dynType.NumMethod()
	if n == 0 {
		fmt.Printf("%s's method set is empty!\n", dynType)
		return
	}
	fmt.Printf("%s's method set:\n", dynType)
	for j := 0; j < n; j++ {
		fmt.Printf("-%s\n", dynType.Method(j).Name)
	}
	fmt.Println()
}

type T struct{}

func (T) M1()  {}
func (T) M2()  {}
func (*T) M3() {}
func (*T) M4() {}

type S T

func main() {

	var n int
	dumpMethodSet(n)
	dumpMethodSet(&n)

	var t T
	dumpMethodSet(t)
	dumpMethodSet(&t)

	var s S
	dumpMethodSet(s)
	dumpMethodSet(&s)
}
