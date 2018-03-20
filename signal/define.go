package signal

import (
	"fmt"
)

// KeyValueStore stores key value pairs, proved simple set get functions
type KeyValueStore interface {
	// Set sets a key value pair
	Set(key string, value interface{})
	// Get gets a value by key
	Get(key string) interface{}
}

// Context is the context for router handler functions
type Context interface {
	// IsNext returns a flag indicates whether the next function should be called
	IsNext() bool
	// Next can set a flag, tell the trigger to call next function
	Next()
	// ResetIsNext resets the next flag, makes IsNext return false
	ResetIsNext()
	// Context needs KeyValueStore to pass payloads
	KeyValueStore
}

// Signal defines a signal dispatcher
type Signal struct {
	PathMap map[string][]func(Context)
}

// Register register a handler function after some path
func (r *Signal) Register(path string, handlers ...func(Context)) {
	if path == "" {
		panic("Invalid path")
	} else if r.PathMap[path] == nil {
		r.PathMap[path] = handlers
	} else {
		for _, h := range handlers {
			r.PathMap[path] = append(r.PathMap[path], h)
		}
	}
}

// Trigger triggers a bunch of handlers registered unser some path
func (r *Signal) Trigger(path string, ctx Context) {
	if path == "" {
		panic("Invalid path")
	} else if r.PathMap[path] == nil {
		panic(fmt.Sprintf("Path %s not exits", path))
	} else {
		for _, h := range r.PathMap[path] {
			h(ctx)
			if ctx.IsNext() {
				ctx.ResetIsNext()
			} else {
				break
			}
		}
	}
}
