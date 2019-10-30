package main

import (
    "time"
)


// A Subscription delivers Items over a channel.  Close cancels the
// subscription, closes the Updates channel, and returns the last fetch error,
// if any.
type Subscription interface {
    Updates() <-chan Item
    Close() error
}



// sub implements the Subscription interface.
type sub struct {
    fetcher Fetcher         // fetches items
    updates chan Item       // sends items to the user
    closing chan chan error // for Close
}

func (s *sub) Updates() <-chan Item {
    return s.updates
}

func (s *sub) Close() error {
    errc := make(chan error)
    s.closing <- errc // HLchan
    return <-errc     // HLchan
}


// loop periodically fecthes Items, sends them on s.updates, and exits
// when Close is called.  It extends dedupeLoop with logic to run
// Fetch asynchronously.
func (s *sub) loop() {
    const maxPending = 10
    type fetchResult struct {
        fetched []Item
        next    time.Time
        err     error
    }
    var fetchDone chan fetchResult // if non-nil, Fetch is running // HL
    var pending []Item
    var next time.Time
    var err error
    var seen = make(map[string]bool)
    for {
        var fetchDelay time.Duration
        if now := time.Now(); next.After(now) {
            fetchDelay = next.Sub(now)
        }
        var startFetch <-chan time.Time
        if fetchDone == nil && len(pending) < maxPending { // HLfetch
            startFetch = time.After(fetchDelay) // enable fetch case
        }
        var first Item
        var updates chan Item
        if len(pending) > 0 {
            first = pending[0]
            updates = s.updates // enable send case
        }
        select {
        case <-startFetch: // HLfetch
            fetchDone = make(chan fetchResult, 1) // HLfetch
            go func() {
                fetched, next, err := s.fetcher.Fetch()
                fetchDone <- fetchResult{fetched, next, err}
            }()
        case result := <-fetchDone: // HLfetch
            fetchDone = nil // HLfetch
            // Use result.fetched, result.next, result.err
            fetched := result.fetched
            next, err = result.next, result.err
            if err != nil {
                next = time.Now().Add(10 * time.Second)
                break
            }
            for _, item := range fetched {
                if id := item.GUID; !seen[id] { // HLdupe
                    pending = append(pending, item)
                    seen[id] = true // HLdupe
                }
            }
        case errc := <-s.closing:
            errc <- err
            close(s.updates)
            return
        case updates <- first:
            pending = pending[1:]
        }
    }
}
