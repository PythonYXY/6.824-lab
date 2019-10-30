package main

import (
    "fmt"
    "math/rand"
    "time"
)

// An Item is a stripped-down RSS item.
type Item struct{ Title, Channel, GUID string }

// A Fetcher fetches Items and returns the time when the next fetch should be
// attempted.  On failure, Fetch returns a non-nil error.
type Fetcher interface {
    Fetch() (items []Item, next time.Time, err error)
}

// Subscribe returns a new Subscription that uses fetcher to fetch Items.
func Subscribe(fetcher Fetcher) Subscription {
    s := &sub{
        fetcher: fetcher,
        updates: make(chan Item),       // for Updates
        closing: make(chan chan error), // for Close
    }
    go s.loop()
    return s
}


type merge struct {
    subs    []Subscription
    updates chan Item
    quit    chan struct{}
    errs    chan error
}

// Merge returns a Subscription that merges the item streams from subs.
// Closing the merged subscription closes subs.
func Merge(subs ...Subscription) Subscription {
    m := &merge{
        subs:    subs,
        updates: make(chan Item, 1),
        quit:    make(chan struct{}),
        errs:    make(chan error),
    }
    for _, sub := range subs {
        go func(s Subscription) {
            for {
                select {
                case m.updates <- <-s.Updates():
                case <-m.quit: // HL
                    m.errs <- s.Close() // HL
                    return              // HL
                }
            }
        }(sub)
    }
    return m
}

func (m *merge) Updates() <-chan Item {
    return m.updates
}

func (m *merge) Close() (err error) {
    close(m.quit) // HL
    for _ = range m.subs {
        if e := <-m.errs; e != nil { // HL
            err = e
        }
    }
    close(m.updates) // HL
    return
}

// Fetch returns a Fetcher for Items from domain.
func Fetch(domain string) Fetcher {
    return fakeFetch(domain)
}


func init() {
    rand.Seed(time.Now().UnixNano())
}

func main() {
    // Subscribe to some feeds, and create a merged update stream.
    merged := Merge(
        Subscribe(Fetch("blog.golang.org")),
        Subscribe(Fetch("googleblog.blogspot.com")),
        Subscribe(Fetch("googledevelopers.blogspot.com")))

    // Close the subscriptions after some time.
    time.AfterFunc(3 * time.Second, func() {
        fmt.Println("closed:", merged.Close())
    })

    // Print the stream.
    for it := range merged.Updates() {
        fmt.Println(it.Channel, it.Title)
    }
}