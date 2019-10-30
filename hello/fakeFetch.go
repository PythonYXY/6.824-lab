package main

import (
    "fmt"
    "math/rand"
    "time"
)


func fakeFetch(domain string) Fetcher {
    return &fakeFetcher{channel: domain}
}

type fakeFetcher struct {
    channel string
    items   []Item
}
 

func (f *fakeFetcher) Fetch() (items []Item, next time.Time, err error) {
    now := time.Now()
    next = now.Add(time.Duration(rand.Intn(5)) * 500 * time.Millisecond)
    item := Item{
        Channel: f.channel,
        Title:   fmt.Sprintf("Item %d", len(f.items)),
    }
    item.GUID = item.Channel + "/" + item.Title
    f.items = append(f.items, item)
    items = []Item{item}
    return
}