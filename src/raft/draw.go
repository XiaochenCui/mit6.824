package raft

import "github.com/fogleman/gg"
import "time"
import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
)

const (
	RoleLeader    = "leader"
	RoleFollower  = "follower"
	RoleCandidate = "candidate"
	OutFile       = "event.log"

	RPCKindHeartbeat   = "heart-beat"
	RPCKindRequestVote = "request-vote"
)

var (
	mu      = &sync.Mutex{}
	created = false
	f       *os.File
	msgChan = make(chan string, 10)
)

type Runner struct {
}

type Event struct {
}

type Call struct {
}

type CallingBody struct {
}

type RPC struct {
	Sender   int
	Receiver int
	Kind     string
	Args     interface{}
	Reply    interface{}
}

type RoleChange struct {
	ID     int
	Before string
	After  string
}

func Drawing() {
	dc := gg.NewContext(1000, 1000)
	dc.DrawCircle(500, 500, 400)
	dc.SetRGB(0, 0, 0)
	dc.Fill()
	dc.SavePNG("out.png")
}

func NewRunner() *Runner {
	return &Runner{}
}

func (r *Runner) Start() {
}

func (r *Runner) Stop() {

}

func InitLogEvent() {
	var err error
	f, err = os.Create(OutFile)
	if err != nil {
		panic(err)
	}

	go Writer()
}

func LogSystemStart() {
	LogEvent("system start", "")
}

func LogRunnerStart(id int) {
	LogEvent("runner start", fmt.Sprintf("%v", id))
}

func LogRunnerStop(id int) {
	LogEvent("runner stop", fmt.Sprintf("%v", id))
}

func LogRPC(sender int, receiver int, kind string, args interface{}, reply interface{}) {
	r := RPC{
		Sender:   sender,
		Receiver: receiver,
		Kind:     kind,
		Args:     args,
		Reply:    reply,
	}
	b, _ := json.Marshal(r)
	LogEvent("rpc", string(b))
}

func LogRoleChange(id int, before, after string) {
	r := RoleChange{
		ID:     id,
		Before: before,
		After:  after,
	}
	b, _ := json.Marshal(r)
	LogEvent("role change", string(b))
}

func LogEvent(name string, content string) {
	t := time.Now()
	var s string
	s += t.Format(time.RFC3339Nano)
	// s += t.String()
	s += "$$" + name + "$$" + content + "\n"
	msgChan <- s
}

func Writer() {
	for {
		s := <-msgChan
		n, err := f.WriteString(s)
		if err != nil {
			panic(err)
		}
		if n != len(s) {
			log.Fatalf("n: %v, length of s: %v", n, len(s))
		}
		err = f.Sync()
		if err != nil {
			panic(err)
		}
	}
}
