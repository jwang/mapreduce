package mapreduce

import (
	"log"
	"os"
	"sync"
)

type reducer struct {
	mu      sync.Mutex
	mappers int
	work    chan interface{}
	result  chan interface{}
}

func newReducer(rFunc Reducer) *reducer {
	r := &reducer{
		work:   make(chan interface{}),
		result: make(chan interface{}),
	}
	go r.run(rFunc)
	return r
}

func (r *reducer) run(rFunc Reducer) {
	err := rFunc(r.work, r.result)
	if err != nil {
		log.Fatal("Reducer error: ", err)
	}
	log.Print("Reducer exited cleanly; shutting down")
	os.Exit(0)
}

func (r *reducer) GetWork(args *struct{}, work *interface{}) os.Error {
	if w, ok := <-r.work; ok {
		*work = w
		return nil
	}
	return ErrDone
}

func (r *reducer) SendResult(result *interface{}, reply *struct{}) os.Error {
	r.result <- *result
	return nil
}

func (r *reducer) Hello(args *struct{}, reply *struct{}) os.Error {
	r.mu.Lock()
	r.mappers++
	r.mu.Unlock()
	return nil
}

func (r *reducer) Goodbye(args *struct{}, reply *struct{}) os.Error {
	r.mu.Lock()
	r.mappers--
	if r.mappers <= 0 {
		close(r.result)
	}
	r.mu.Unlock()
	return nil
}
