package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-redis/redis"
)

var ignoreBasePatterns = []string{
	"ccache.conf*",
	"stats*",
	"*.tmp.*",
	"*.o.*",
	"*.manifest.*",
	"CACHEDIR.TAG",
}

var ignoreDirPatterns = []string{
	"tmp",
}

func shouldIgnore(name string) bool {
	for _, p := range ignoreBasePatterns {
		base := filepath.Base(name)
		match, err := filepath.Match(p, base)
		if err != nil {
			log.Fatal(err)
		}
		if match {
			return true
		}
	}
	for _, p := range ignoreDirPatterns {
		base := filepath.Dir(name)
		match, err := filepath.Match(p, base)
		if err != nil {
			log.Fatal(err)
		}
		if match {
			return true
		}
	}
	return false
}

const (
	redisChannelName = "ccache"
)

type watcher struct {
	ctx context.Context
	w   *fsnotify.Watcher
	c   chan fsnotify.Event
}

func newWatcher(ctx context.Context) *watcher {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}

	err = filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			return nil
		}

		err = w.Add(path)
		if err != nil {
			log.Fatal(err)
		}
		return nil
	})

	// Pass events downstream
	c := make(chan fsnotify.Event)

	rv := &watcher{
		ctx: ctx,
		w:   w,
		c:   c,
	}

	return rv
}

func (w *watcher) loop() {
	defer w.w.Close()

	for {
		select {
		case <-w.ctx.Done():
			log.Println("inotify exiting...")
			return
		case event := <-w.w.Events:
			if shouldIgnore(event.Name) {
				break
			}
			if event.Op&fsnotify.Create == fsnotify.Create {
				w.c <- event
			}
		case err := <-w.w.Errors:
			log.Println("inotify error:", err)
		}
	}
}

type handler struct {
	ctx context.Context
	c   <-chan fsnotify.Event
	r   *redis.Client
	s   *redis.PubSub

	// Keep map of published strings so we can filter our own messages
	published map[string]int

	// Keep map of created files to we can filter inotify events
	created map[string]int
}

var redisAddr = flag.String("redis", "localhost:6379", "Redis address")

func newHandler(ctx context.Context, c <-chan fsnotify.Event) *handler {
	r := redis.NewClient(&redis.Options{Addr: *redisAddr})
	_, err := r.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

	rv := &handler{
		ctx: ctx,
		c:   c,
		r:   r,
		s:   r.Subscribe(redisChannelName),

		published: make(map[string]int),
		created:   make(map[string]int),
	}
	return rv
}

func (h *handler) publish(value string) {
	var err error

	err = h.r.Publish(redisChannelName, value).Err()
	if err != nil {
		log.Printf("handler: unable to publish to Redis channel: %s", err)
		return
	}

	// Ignore messages for this value
	h.published[value]++
}

func (h *handler) wasPublished(value string) bool {
	if v, ok := h.published[value]; ok {
		// Clean up once this function has been called as many
		// times as the value has been published by this process.
		if v <= 1 {
			delete(h.published, value)
		}
		return true
	}
	return false
}

func (h *handler) create(name string, buf []byte) {
	// Write to temporary file
	tmpName := fmt.Sprintf("%s.tmp.%d", name, os.Getpid())
	f, err := os.OpenFile(tmpName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("handler: %s", err)
		return
	}

	defer f.Close()

	_, err = f.Write(buf)
	if err != nil {
		log.Printf("handler: %s", err)

		// Clean up
		f.Close()
		os.Remove(tmpName)
		return
	}

	f.Close()

	// Ignore inotify events for this file
	h.created[name]++

	// Rename to real file
	err = os.Rename(tmpName, name)
	if err != nil {
		log.Printf("handler: %s", err)
		return
	}
}

func (h *handler) wasCreated(name string) bool {
	if v, ok := h.created[name]; ok {
		// Clean up once this function has been called as many
		// times as the value has been published by this process.
		if v <= 1 {
			delete(h.created, name)
		}
		return true
	}
	return false
}

func (h *handler) handleCreate(e fsnotify.Event) {
	var err error
	var name = e.Name

	// Ignore if this was created by ourselves
	if h.wasCreated(name) {
		return
	}

	log.Printf("handler: (local) create %s", e.Name)

	// Open newly cached file
	f, err := os.Open(e.Name)
	if err != nil {
		log.Printf("handler: %s", err)
		return
	}

	defer f.Close()

	buf, err := ioutil.ReadAll(f)
	if err != nil {
		log.Printf("handler: unable to read %s: %s", e.Name, err)
		return
	}

	// Write contents of file to Redis
	err = h.r.Set(e.Name, buf, time.Minute).Err()
	if err != nil {
		log.Printf("handler: unable to set Redis key: %s", err)
		return
	}

	// Publish presence of file to other clients
	h.publish(e.Name)
}

func (h *handler) handlePublish(m *redis.Message) {
	var err error
	var name = m.Payload

	// Ignore if this was published by ourselves
	if h.wasPublished(name) {
		return
	}

	log.Printf("handler: (subscription) create %s", name)

	// Retrieve contents of file from Redis
	buf, err := h.r.Get(name).Bytes()
	if err != nil {
		log.Printf("handler: unable to get Redis key: %s", err)
		return
	}

	// Sanity check
	if len(buf) == 0 {
		log.Printf("handler: empty Redis key: %s", name)
		return
	}

	h.create(name, buf)
}

func (h *handler) loop() {
	defer h.s.Close()

	// Channel for subscription messages
	sub := h.s.Channel()

	for {
		select {
		case <-h.ctx.Done():
			log.Println("handler exiting...")
			return
		case e := <-h.c:
			h.handleCreate(e)
		case m := <-sub:
			h.handlePublish(m)
		}
	}
}

func main() {
	var wg sync.WaitGroup

	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())

	// Start watcher
	wg.Add(1)
	w := newWatcher(ctx)
	go func() {
		w.loop()
		wg.Done()
	}()

	// Start consumer
	wg.Add(1)
	h := newHandler(ctx, w.c)
	go func() {
		h.loop()
		wg.Done()
	}()

	log.Println("started")

	// Wait for SIGINT
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// Cleanup
	cancel()
	wg.Wait()
}
