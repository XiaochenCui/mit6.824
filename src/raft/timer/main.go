package main

import (
	"fmt"
	"time"
)

func main() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for i:=0; i<10; i++{
			<-ticker.C
			fmt.Println("tick")
		}
	}()

	time.Sleep(200 * time.Millisecond)

	timer := time.NewTimer(time.Second)
	go func() {
		<-timer.C
		for i:=0; i<3; i++{
			// fmt.Println("start reset")
			timer.Reset(2 * time.Second)
			time.Sleep(2 * time.Second)
			timer = time.NewTimer(2 * time.Second)
			// timer.Stop()
			// timer.Reset(2 * time.Second)
			<-timer.C
			fmt.Println("reset")
		}
		fmt.Println("Timer expired")
	}()

	time.Sleep(11 * time.Second)
}