package main

import (
	"fmt"
	"reflect"
)

type E1 struct {
}

func (E1) M1() {

}
func (E1) M2() {

}
func (E1) M3() {

}

type E2 struct {
}

func (E2) M1() {

}
func (E2) M2() {

}
func (E1) M4() {

}

type T struct {
	E1
	E2
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

func main() {

	t := T{}
	t.E1.M1()
	t.E2.M2()
}
