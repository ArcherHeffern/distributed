package main

import (
	"time"
	"fmt"
	"github.com/leonelquinteros/gorand"
)

func main() {
	time := time.Now().UnixNano()
	uuid, err := gorand.UUIDv4()
	if err != nil {
		panic(err.Error())
	}
	out := fmt.Sprintf("%d%s", time, uuid)
	print(out)
}
