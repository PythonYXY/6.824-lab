package raft

import (
	"fmt"
	"log"
	"time"
)
import "math/rand"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func randInt(min int, max int) int {
	return min + rand.Intn(max - min)
}

func (rf *Raft) update(currentState State, currentTerm int, votedFor int, lastUpdatedTime time.Time) {
	rf.currentState = currentState
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastUpdatedTime = lastUpdatedTime
}

func (rf *Raft) printStatus() {
	fmt.Println("===========================")
	fmt.Printf("id: %v\n", rf.me)
	fmt.Printf("currentState: %v\n", rf.currentState)
	fmt.Printf("currentTerm: %v\n", rf.currentTerm)
	fmt.Printf("votedFor: %v\n", rf.votedFor)
	fmt.Printf("log: %v\n", rf.log)
	fmt.Println("===========================")
}