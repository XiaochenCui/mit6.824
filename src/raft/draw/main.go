package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"raft"
	"sort"
	"strconv"
	"time"

	"github.com/fogleman/gg"
	"github.com/golang/freetype/truetype"
	"golang.org/x/image/font"
	"golang.org/x/image/font/gofont/gomono"
)

var (
	// PointR = 7
	dc         *gg.Context
	interval   float64
	leftBlank  = float64(300)
	arrowRatio = float64(10)

	lineWidth = float64(1.5)

	runnerXMap = make(map[int]float64)

	baseTime     time.Time
	endTime      time.Time
	baseTimeUnix float64
	endTimeUnix  float64

	runnerIntervals = make(map[int][]Interval)

	// log file
	logContent []string

	// image size
	W = 1424
	H int

	margin = float64(30)

	runner []int

	stringQueue []String

	narmalFace font.Face
)

type String struct {
	S     string
	X     float64
	Y     float64
	Color string
}

type Interval struct {
	Start time.Time
	End   time.Time
}

func init() {
	log.SetOutput(os.Stdout)

	initFont()
}

func initFont() {
	f, err := truetype.Parse(gomono.TTF)
	if err != nil {
		log.Fatal(err)
	}

	narmalFace = truetype.NewFace(f, &truetype.Options{Size: 16})
}

func main() {
	ReadLog()
	Preprocess()

	Drawing()

	RenderingString()
	save()
}

func ReadLog() {
	filename := "event.log"
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

func Preprocess() {

	intervalsMap := make(map[int][]Interval)
	for _, line := range logContent {
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
			if !contains(runner, id) {
				runner = append(runner, id)
			}
			is := intervalsMap[id]
			if len(is) > 0 {
				is[len(is)-1].End = t
			}
			intervalsMap[id] = append(is, Interval{Start: t})
		}
	}
	// log.Panic(intervalsMap)

	baseTime = baseTime.Truncate(200 * time.Millisecond)
	endTime = endTime.Add(1 * time.Second)

	interval = float64(900 / (len(runner) - 1))
	// log.Panic(runner)
	for i, id := range runner {
		x := leftBlank + float64(i)*interval
		runnerXMap[id] = float64(x)
	}
	log.Print(runnerXMap)

	totalSeconds := int(endTime.Sub(baseTime).Seconds())
	H = 1200 + totalSeconds*1000
	log.Print(endTime)
	log.Print(TimeToY(endTime))
	log.Print(H)

	// H = int(TimeToY(endTime)) + 50
	// log.Print(endTime)
	log.Print(TimeToY(endTime))
	// log.Panic(H)

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

	// log.Panicf("interval map: %v", intervalsMap)
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
		} else {
			for _, interval := range intervals {
				start := TimeToY(interval.Start)
				end := float64(H - 20)
				if !interval.End.IsZero() {
					end = TimeToY(interval.End)
				}

				log.Print(start, end)
				dc.SetHexColor("#3da4ab")
				dc.SetLineWidth(3)
				dc.DrawLine(x, start, x, end)
				dc.Stroke()
			}
		}
	}

}

func Drawing() {
	for _, line := range logContent {
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
			DrawString(s, x+10, y, "#0e9aa7")

		case "rpc":
			rpc := raft.RPC{}
			log.Print(msg)
			err := json.Unmarshal([]byte(msg), &rpc)
			if err != nil {
				panic(err)
			}
			log.Print(rpc)
			log.Print(rpc.Args)

			y := TimeToY(t)
			start := runnerXMap[rpc.Sender]
			end := runnerXMap[rpc.Receiver]

			// x := (start + end) / 2 - 100
			// DrawString(raft.MapToString(rpc.Args), y, x)

			switch rpc.Kind {
			case raft.RPCKindHeartbeat:
				DrawArrow("#b3cde0", y, start, end, 2)

			case raft.RPCKindRequestVote:
				DrawArrow("#fe8a71", y, start, end, 0)

			case raft.RPCKindAppendEntry:
				DrawArrow("#6497b1", y, start, end, 0)
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
			dc.SetHexColor("#f37736")
			dc.DrawPoint(x, y, 7)
			dc.Fill()
			dc.Stroke()

			s := fmt.Sprintf("%s -> %s", rc.Before, rc.After)
			if s[0:1] == "c" {
				y += 12
			} else {
				y -= 2
			}
			DrawString(s, x+10, y, "#f37736")

		case "term up":
			rc := raft.TermUp{}
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
			dc.SetHexColor("#fed766")
			dc.DrawPoint(x, y, 7)
			dc.Fill()
			dc.Stroke()

			s := fmt.Sprintf("term up: %d -> %d", rc.Before, rc.After)
			y += 12
			x += 15
			DrawString(s, x+10, y, "#fed766")

		case "connect":
			y := TimeToY(t)
			id, _ := strconv.Atoi(msg)
			x := runnerXMap[id]

			dc.SetHexColor("#7bc043")
			dc.DrawPoint(x, y, 9)
			dc.Fill()
			dc.Stroke()

			s := fmt.Sprintf("%d connected", id)
			DrawString(s, x+10, y, "#7bc043")

		case "disconnect":
			y := TimeToY(t)
			id, _ := strconv.Atoi(msg)
			x := runnerXMap[id]

			dc.SetHexColor("#ee4035")
			dc.DrawPoint(x, y, 9)
			dc.Fill()
			dc.Stroke()

			s := fmt.Sprintf("%d disconnected", id)
			DrawString(s, x+10, y, "#ee4035")
		}
	}

	DrawTimeSeries(baseTime, endTime)

	dc.SetRGBA(50, 50, 50, 1)
	dc.SetHexColor("#4a4e4d")
	dc.SetLineWidth(10)
	dc.DrawLine(margin, margin, float64(W)-margin, margin)
	dc.Stroke()

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
	diff := t.Sub(baseTime)
	ms := float64(diff.Nanoseconds()) / math.Pow10(6)
	offset := float64(35)
	return ms + offset + margin

	// tUnix := float64(t.UnixNano())
	// ratio := float64(tUnix-baseTimeUnix) / float64(endTimeUnix-baseTimeUnix)
	// r := 35 + float64(H-70)*ratio
	// return r
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
	width := lineWidth + widthBuf*2
	dc.SetLineWidth(width)

	if fixLineWidth > 0 {
		dc.SetLineWidth(fixLineWidth)
	}

	dc.SetHexColor(color)
	if start < end {
		dc.DrawLine(end-arrowRatio, y-arrowRatio, end, y)
		dc.DrawLine(end-arrowRatio, y+arrowRatio, end, y)
	} else {
		dc.DrawLine(end+arrowRatio, y-arrowRatio, end, y)
		dc.DrawLine(end+arrowRatio, y+arrowRatio, end, y)
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
		DrawString(s, 160, y, "#ff6f69")

		base = base.Add(200 * time.Millisecond)
	}
}

func DrawString(s string, x, y float64, color string) {
	se := String{
		S:     s,
		X:     x,
		Y:     y,
		Color: color,
	}
	stringQueue = append(stringQueue, se)
}

func RenderingString() {
	log.Print(stringQueue)
	sort.Slice(stringQueue, func(i, j int) bool { return stringQueue[i].Y < stringQueue[j].Y })

	m := make(map[float64][]String)
	for _, se := range stringQueue {
		l := m[se.X]
		lastY := float64(0)
		if len(l) > 0 {
			lastY = l[len(l)-1].Y
		}
		if se.Y-lastY < 20 {
			se.Y = lastY + 20
		}
		m[se.X] = append(m[se.X], se)
	}
	for k, v := range m {
		log.Print()
		log.Print(k)
		log.Print(v)
	}
	log.Print(m)
	for _, v := range m {
		for _, se := range v {
			dc.SetHexColor(se.Color)
			dc.DrawString(se.S, se.X, se.Y)
		}
	}
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func save() {
	dc.SavePNG("out.png")
}
