package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	ch := make(chan *DoTaskArgs)
	quit := make(chan int)
	var wg sync.WaitGroup

	go func() {
		for {
			select {
			case wk := <- registerChan:
				wg.Add(1)
				go func() {
					for {
						select {
						case args, ok := <- ch:
							if ok {
								res  := call(wk, "Worker.DoTask", args, nil)

								// handling worker failures
								if !res {
									ch <- args
									wg.Done()
									return
								}
							} else {
								wg.Done()
								return
							}
						}
					}
				}()
			case <- quit:
				return
			}
		}
	}()


	for i := 0; i < ntasks; i++ {
		args := &DoTaskArgs {
			jobName,
			mapFiles[i], // File: only for map, the input file
			phase, // Phase: are we in mapPhase or reducePhase?
			i, // TaskNumber: this task's index in the current phase
			n_other} // NumOtherPhase is the total number of tasks in other phase; mappers need this to compute the number of output bins, and reducers needs this to know how many input files to collect.

		ch <- args
	}

	close(ch)
	wg.Wait()
	quit <- 1

	fmt.Printf("Schedule: %v done\n", phase)
}