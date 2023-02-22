package kafka

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type finder struct {
	cancel      context.CancelFunc
	found       atomic.Uint32
	checked     atomic.Uint64
	limitSearch uint64
	limitFind   uint32
	containing  [][]byte
	containType int
	res         [][]byte
	mux         *sync.Mutex
	wg          *sync.WaitGroup
}

func newFinder(findLimit, searchLimit, containType int, containing [][]byte, cancelFunc context.CancelFunc) *finder {
	f := &finder{
		cancel:      cancelFunc,
		found:       atomic.Uint32{},
		checked:     atomic.Uint64{},
		limitSearch: uint64(searchLimit),
		limitFind:   uint32(findLimit),
		containing:  containing,
		containType: containType,
		res:         make([][]byte, 0),
		mux:         &sync.Mutex{},
		wg:          &sync.WaitGroup{},
	}
	go f.statusChecker()

	return f
}

func (f *finder) statusChecker() {
	ticker := time.NewTicker(time.Second)

	for {
		<-ticker.C
		if f.found.Load() >= f.limitFind || f.checked.Load() >= f.limitSearch {
			f.cancel()
			return
		}
	}
}

func (f *finder) handle(msg []byte) {
	f.checked.Add(1)
	if f.containType == ContainTypeAll {
		for _, contain := range f.containing {
			if !bytes.Contains(msg, contain) {
				return
			}
		}
		f.found.Add(1)
		f.mux.Lock()
		f.res = append(f.res, msg)
		f.mux.Unlock()
		return
	}
	if f.containType == ContainTypeAny {
		for _, contain := range f.containing {
			if bytes.Contains(msg, contain) {
				f.found.Add(1)
				f.mux.Lock()
				f.res = append(f.res, msg)
				f.mux.Unlock()
				return
			}
		}
		return
	}
}
