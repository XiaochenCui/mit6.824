package main

import "github.com/fogleman/gg"

// import "math/rand"

import (
	"encoding/json"
	"math"

	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"time"

	// "encoding/json"
	"os"

	"github.com/golang/freetype/truetype"
	// "golang.org/x/image/font/gofont/goregular"
	"golang.org/x/image/font/gofont/gomono"

	"raft"
)

const (
	W = 1524
	H = 1124
	// PointR = 7
)

var (
	dc        *gg.Context
	interval  float64
	leftBlank = float64(300)

	lineWidth = float64(3)

	runnerXMap = make(map[int]float64)

	baseTime     time.Time
	endTime      time.Time
	baseTimeUnix float64
	endTimeUnix  float64
)

type Interval struct {
	Start time.Time
	End   time.Time
}

func init() {
	log.SetOutput(os.Stdout)
}

func main() {

	// dc := gg.NewContext(1024, 1024)
	// dc.SetRGB(1, 1, 1)
	// dc.Clear()
	// dc.SetRGB(0, 0, 0)
	// dc.DrawStringAnchored("Hello, world!", 512, 512, 0.5, 0.5)
	// dc.SavePNG("out.png")

	Drawing()
}

func Drawing() {
	filename := "event.log"
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Error: %v", err)
	}

	var contents []string

	fileScanner := bufio.NewScanner(file)
	for fileScanner.Scan() {
		contents = append(contents, fileScanner.Text())
	}

	// drawing init
	dc = gg.NewContext(W, H)
	// dc.SetRGB255(0, 0, 0)
	dc.SetRGB255(255, 255, 255)
	// dc.SetRGB255(50, 50, 50)
	dc.Clear()

	font, err := truetype.Parse(gomono.TTF)
	if err != nil {
		log.Fatal(err)
	}

	face := truetype.NewFace(font, &truetype.Options{Size: 16})
	dc.SetFontFace(face)

	var runner []int
	intervalsMap := make(map[int][]Interval)
	for _, line := range contents {
		t, header, msg := ParseLine(line)
		log.Print(t, header, msg)

		tUnix := float64(t.UnixNano())
		if tUnix > endTimeUnix {
			endTime = t
			endTimeUnix = tUnix
		}

		switch header {
		case "system start":
			baseTime = t
			baseTimeUnix = tUnix
		case "runner start":
			id, _ := strconv.Atoi(msg)
			runner = append(runner, id)
			is := intervalsMap[id]
			intervalsMap[id] = append(is, Interval{Start: t})
		}
	}

	interval = float64(900 / (len(runner) - 1))
	for i, id := range runner {
		x := leftBlank + float64(i)*interval
		runnerXMap[id] = float64(x)
	}
	log.Print(runnerXMap)

	for id, intervals := range intervalsMap {
		x := runnerXMap[id]
		if len(intervals) == 1 {
			i := intervals[0]
			start := TimeToY(i.Start)
			end := float64(H - 20)
			if !i.End.IsZero() {
				end = TimeToY(i.End)
			}

			log.Print(start, end)
			dc.SetHexColor("#3da4ab")
			dc.SetLineWidth(3)
			dc.DrawLine(x, start, x, end)
			dc.Stroke()
		}
	}

	for _, line := range contents {
		t, header, msg := ParseLine(line)
		// log.Print(t, header, msg)

		switch header {
		case "system start":

		case "runner start":
			y := TimeToY(t)
			id, _ := strconv.Atoi(msg)
			x := runnerXMap[id]

			dc.SetHexColor("#0e9aa7")
			dc.DrawPoint(x, y, 7)
			dc.Fill()
			dc.Stroke()

			s := fmt.Sprintf("runner %d started", id)
			dc.DrawString(s, x+10, y)

		case "rpc":
			rpc := raft.RPC{}
			log.Print(msg)
			err := json.Unmarshal([]byte(msg), &rpc)
			if err != nil {
				panic(err)
			}
			log.Print(rpc)

			y := TimeToY(t)
			start := runnerXMap[rpc.Sender]
            end := runnerXMap[rpc.Receiver]
            
            switch rpc.Kind {
            case raft.RPCKindHeartbeat:
                DrawArrow("#b3cde0", y, start, end, 2)

            case raft.RPCKindRequestVote:
                DrawArrow("#fe8a71", y, start, end, 0)
            }

		case "role change":
			rc := raft.RoleChange{}
			err := json.Unmarshal([]byte(msg), &rc)
			if err != nil {
				panic(err)
			}

			if rc.Before == rc.After {
				continue
			}

			y := TimeToY(t)
			x := runnerXMap[rc.ID]

			// log.Printf("id: %v, x: %v", id, x)
			dc.SetHexColor("#fe4a49")
			dc.DrawPoint(x, y, 7)
			dc.Fill()
			dc.Stroke()

			s := fmt.Sprintf("%s -> %s", rc.Before, rc.After)
			dc.DrawString(s, x+10, y)
		}
	}

	DrawTimeSeries(baseTime, endTime)

	dc.SetRGBA(50, 50, 50, 1)
	dc.SetHexColor("#4a4e4d")
	dc.SetLineWidth(10)
	dc.DrawLine(20, 15, 1000, 15)
	dc.Stroke()

	// for i := 0; i < 1000; i++ {
	// 	x1 := rand.Float64() * W
	// 	y1 := rand.Float64() * H
	// 	x2 := rand.Float64() * W
	// 	y2 := rand.Float64() * H
	// 	r := rand.Float64()
	// 	g := rand.Float64()
	// 	b := rand.Float64()
	// 	a := rand.Float64()*0.5 + 0.5
	// 	w := rand.Float64()*4 + 1
	// 	dc.SetRGBA(r, g, b, a)
	// 	dc.SetLineWidth(w)
	// 	dc.DrawLine(x1, y1, x2, y2)
	// 	dc.Stroke()
	// }
	dc.SavePNG("out.png")
}

func ParseLine(s string) (time.Time, string, string) {
	byteArr := bytes.Split([]byte(s), []byte("$$"))
	timeString := string(byteArr[0])
	log.Print(timeString)
	t, _ := time.Parse(time.RFC3339Nano, string(timeString))
	log.Print(t)
	header := string(byteArr[1])
	msg := string(byteArr[2])
	return t, header, msg
}

func TimeToY(t time.Time) float64 {
	tUnix := float64(t.UnixNano())
	ratio := float64(tUnix-baseTimeUnix) / float64(endTimeUnix-baseTimeUnix)
	r := 35 + 1000*ratio
	return r
}

func DrawArrow(color string, y, start, end float64, fixLineWidth float64) {

	// shift
	shift := float64(16)
	if start < end {
		start += shift
		end -= shift
	} else {
		start -= shift
		end += shift
	}

	widthBuf := float64(len(runnerXMap)) - math.Abs(start-end)/interval
	width := lineWidth + widthBuf*2.5
    dc.SetLineWidth(width)
    
    if fixLineWidth > 0 {
        dc.SetLineWidth(fixLineWidth)
    }

    dc.SetHexColor(color)
	if start < end {
		dc.DrawLine(end-20, y-20, end, y)
		dc.DrawLine(end-20, y+20, end, y)
	} else {
		dc.DrawLine(end+20, y-20, end, y)
		dc.DrawLine(end+20, y+20, end, y)
	}

	log.Print(start, end)
	dc.DrawLine(start, y, end, y)
	dc.Stroke()

	dc.SetLineWidth(lineWidth)
}

func DrawTimeSeries(base, end time.Time) {
    diff := end.Sub(base)
    n := int(diff.Seconds() / 0.2)
    log.Printf("diff: %v, n: %v", diff, n)

	for i := 0; i < n; i++ {
        s := base.Format("15:04:05.000")
        log.Print(s)

        y := TimeToY(base)
        log.Print(y)
        dc.DrawString(s, 160, y)

		base = base.Add(200 * time.Millisecond)
	}
}
