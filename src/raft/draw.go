package raft

import "github.com/fogleman/gg"
import "time"
import (
	// "bytes"
	// "net/http"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	// "sync"
	// "github.com/go-redis/redis"
)

const (
	RoleLeader    = "leader"
	RoleFollower  = "follower"
	RoleCandidate = "candidate"
	OutFile       = "event.log"

	RPCKindHeartbeat   = "heart-beat"
	RPCKindRequestVote = "request-vote"
	RPCKindAppendEntry = "append-entry"
)

var (
	RoleMap = map[int32]string{
		LEADER:    RoleLeader,
		FOLLOWER:  RoleFollower,
		CANDIDATE: RoleCandidate,
	}
	// mu      = &sync.Mutex{}
	created = false
	f       *os.File
	msgChan = make(chan string, 10)

	// redisClient *redis.Client
)

type Runner struct {
}

type Event struct {
	ID      int
	Name    string
	Content string
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

type TermUp struct {
	ID     int
	Before int
	After  int
}

type AttrChange struct {
	ID     int
	Name   string
	Before interface{}
	After  interface{}
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

	// go Writer()
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

func LogTermUp(id int, before, after int) {
	r := TermUp{
		ID:     id,
		Before: before,
		After:  after,
	}
	b, _ := json.Marshal(r)
	LogEvent("term up", string(b))
}

// func LogCommitChange(id int, name string, s string) {
// 	e := Event{
// 		ID:     id,
// 		Name:   "commit change",
// 		Content: s,
// 	}
// 	b, _ := json.Marshal(e)
// 	LogEvent("attr change", string(b))
// }

func LogAttrChange(id int, name string, before, after interface{}) {
	r := AttrChange{
		ID:     id,
		Name:   name,
		Before: before,
		After:  after,
	}
	b, _ := json.Marshal(r)
	LogEvent("attr change", string(b))
}

func LogConnect(id int) {
	LogEvent("connect", fmt.Sprintf("%v", id))
}

func LogDisconnect(id int) {
	LogEvent("disconnect", fmt.Sprintf("%v", id))
}

func LogApply(id int, start, end int) {
	LogEvent("apply", fmt.Sprintf("%v:%v:%v", id, start, end))
}

// func LogEvent(name string, content string) {
// 	go logEvent(name, content)
// }

// func LogEventStruct(e Event) {

// }

func LogEvent(name string, content string) {
	t := time.Now()
	var s string
	s += t.Format(time.RFC3339Nano)
	s += "$$" + name + "$$" + content + "\n"

	// 1, local write
	// msgChan <- s
	log.Print(s)

	// 2, http
	// _, err := http.Post("http://localhost:60000", "text/plain", bytes.NewBuffer([]byte(s)))
	// if err != nil {
	// 	panic(err)
	// }

	// 3, redis
	// redisClient.HSet("log", "1", s)

	// end := time.Now()
	// consume := end.Sub(t)
	// // log.Printf("log consume %v seconds", consume.Seconds())
	// if consume.Seconds() > 0.001 {
	// 	panic(consume)
	// }
}

func Writer() {
	for {
		s := <-msgChan
		n, err := f.WriteString(s)
		if err != nil {
			panic(err)
		}
		if n != len(s) {
			// log.Fatalf("n: %v, length of s: %v", n, len(s))
		}
		// err = f.Sync()
		// if err != nil {
		// 	panic(err)
		// }
	}
}
