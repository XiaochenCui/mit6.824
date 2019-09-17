package raft

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"sort"
	"time"

	"github.com/fatih/structs"
)

// Debugging
const Debug = 0

func init() {
	rand.Seed(time.Now().UnixNano())
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type KeyValue struct {
	Key   string
	Value interface{}
}

func StructToString(originStruct interface{}) string {
	var r string
	s := structs.New(originStruct)

	m := s.Map()

	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	kvs := make([]KeyValue, 0, len(m))
	for _, key := range keys {
		kv := KeyValue{
			Key:   key,
			Value: s.Field(key).Value(),
		}
		kvs = append(kvs, kv)
	}

	r += s.Name()
	r += "("
	for _, kv := range kvs {
		r += kv.Key
		r += ":"
		r += fmt.Sprintf("%v", kv.Value)
		r += ", "
	}
	r = r[:len(r)-2]
	r += ")"
	return r
}

func MapToString(m interface{}) string {
	v := reflect.ValueOf(m)
	if v.Kind() != reflect.Map {
		panic(fmt.Errorf("arg kind is %v", v.Kind()))
	}
	var r string
	r += "("
	for _, key := range v.MapKeys() {
		r += key.String()
		r += ":"
		strct := v.MapIndex(key)
		r += fmt.Sprintf("%v", strct)
		r += ", "
	}
	r = r[:len(r)-2]
	r += ")"
	return r
}

func MapGet(m interface{}, k string) interface{} {
	v := reflect.ValueOf(m)
	if v.Kind() != reflect.Map {
		panic(v.Kind())
	}
	for _, key := range v.MapKeys() {
		s := key.String()
		if s == k {
			value := v.MapIndex(key)
			return value
		}
	}
	return nil
}

func RandomInt(min, max int) int {
	return rand.Intn(max-min) + min
}
