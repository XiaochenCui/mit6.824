package main

import (
	"bufio"
	"regexp"

	// "bytes"
	// "encoding/json"
	// "fmt"
	"log"
	// "math"
	"os"
	"raft"
	"strconv"
	"time"

	"github.com/fogleman/gg"
	// "github.com/golang/freetype/truetype"
	// "golang.org/x/image/font/gofont/gomono"
)

var (
	// PointR = 7
	dc         *gg.Context
	interval   float64
	leftBlank  = float64(300)
	arrowRatio = float64(10)

	lineWidth = float64(3)

	runnerXMap = make(map[int]float64)

	baseTime     time.Time
	endTime      time.Time
	baseTimeUnix float64
	endTimeUnix  float64

	// log file
	logContent []string

	// image size
	W = 1524
	H int

	runners = make(map[int]bool)

	locks           = make(map[int][]*Lock)
	acquireIndex    = make(map[int]int)
	acquireSucIndex = make(map[int]int)
	releaseIndex    = make(map[int]int)
	releaseSucIndex = make(map[int]int)
)

type Lock struct {
	Runner         int
	AcquireTime    time.Time
	AcquireSucTime time.Time
	ReleaseTime    time.Time
	ReleaseSucTime time.Time
}

func init() {
	log.SetOutput(os.Stdout)
}

func main() {
	ReadLog()
	PreProcess()
}

func ReadLog() {
	filename := "out"
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		log.Printf("Error: %v", err)
	}

	fileScanner := bufio.NewScanner(file)
	for fileScanner.Scan() {
		logContent = append(logContent, fileScanner.Text())
	}
}

func PreProcess() {
	tFormat := "2006/01/02 15:04:05.999999"
	timeLen := len(tFormat)

	exp := regexp.MustCompile(`.+raft instance (?P<runner>\d+)\s(?P<action>\w+)\slock\s(?P<result>\w+).*line\s(?P<line>\d+)`)

	for _, c := range logContent {
		if len(c) < timeLen {
			continue
		}
		tStr := c[0:timeLen]
		t, err := time.Parse(tFormat, tStr)
		if err != nil {
			continue
		}
		log.Print(t)

		match := exp.FindStringSubmatch(c)
		// log.Print(match)
		if len(match) < 1 {
			continue
		}

		result := make(map[string]string)
		for i, name := range exp.SubexpNames() {
			if i != 0 && name != "" {
				result[name] = match[i]
			}
		}
		log.Print(result)

		runner, _ := strconv.Atoi(result["runner"])
		// line, _ := strconv.Atoi(result["line"])
		r := result["result"]
		action := result["action"]

		if _, ok := runners[runner]; !ok {
			locks[runner] = []*Lock{}
			acquireSucIndex[runner] = 0

			runners[runner] = true
		}

		var l *Lock
		if action == "acquire" {
			if r == "at" {
				lock := Lock{
					Runner:      runner,
					AcquireTime: t,
				}
				locks[runner] = append(locks[runner], &lock)
			} else {
				index := acquireSucIndex[runner]
				l = locks[runner][index]
				l.AcquireSucTime = t
				acquireSucIndex[runner]++
			}
		} else {
			if r == "at" {
				index := releaseIndex[runner]
				l = locks[runner][index]
				l.ReleaseTime = t
				releaseIndex[runner]++
			} else {
				index := releaseSucIndex[runner]
				l = locks[runner][index]
				l.ReleaseSucTime = t
				releaseSucIndex[runner]++
			}
		}
	}
	log.Print(locks)
	for k, v := range locks {
		log.Printf("runner %d", k)
		for _, l := range v {
			log.Print(raft.StructToString(l))
		}
	}

	for k, v := range locks {
		log.Printf("%v have %d locks in total", k, len(v))
	}

	// log.Print(acquireIndex)
	log.Print(acquireSucIndex)
	log.Print(releaseIndex)
	log.Print(releaseSucIndex)

	var maxD time.Duration
	var minorMaxD time.Duration
	var maxL *Lock
	var minorMaxL *Lock

	log.Print("-------------------------")
	log.Print("-------------------------")
	for k, v := range locks {
		// log.Printf("runner %d", k)
		minorMaxD = 0
		for _, l := range v {
			newD := l.AcquireSucTime.Sub(l.AcquireTime)
			if newD > maxD {
				maxD = newD
				maxL = l
			}
			if newD > minorMaxD {
				minorMaxD = newD
				minorMaxL = l
			}
		}
		log.Printf("max acquire time of %v: %v, lock: %v", k, minorMaxD, raft.StructToString(minorMaxL))
	}
	log.Printf("max acquire time: %v, lock: %v", maxD, raft.StructToString(maxL))

	log.Print("-------------------------")
	log.Print("-------------------------")
	maxD = 0
	for k, v := range locks {
		// log.Printf("runner %d", k)
		minorMaxD = 0
		for _, l := range v {
			newD := l.ReleaseTime.Sub(l.AcquireSucTime)
			if newD > maxD {
				maxD = newD
				maxL = l
			}
			if newD > minorMaxD {
				minorMaxD = newD
				minorMaxL = l
			}
		}
		log.Printf("max hold time of %v: %v, lock: %v", k, minorMaxD, raft.StructToString(minorMaxL))
	}
	log.Printf("max hold time: %v, lock: %v", maxD, raft.StructToString(maxL))

	log.Print("-------------------------")
	log.Print("-------------------------")
	maxD = 0
	for k, v := range locks {
		// log.Printf("runner %d", k)
		minorMaxD = 0
		for _, l := range v {
			newD := l.ReleaseSucTime.Sub(l.ReleaseTime)
			if newD > maxD {
				maxD = newD
				maxL = l
			}
			if newD > minorMaxD {
				minorMaxD = newD
				minorMaxL = l
			}
		}
		log.Printf("max release time of %v: %v, lock: %v", k, minorMaxD, raft.StructToString(minorMaxL))
	}
	log.Printf("max release time: %v, lock: %v", maxD, raft.StructToString(maxL))
}
