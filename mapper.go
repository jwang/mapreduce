package mapreduce

import (
	"log"
	"rpc"
)

type e struct{}

func runMapper(reducer string, mFunc Mapper) {
	c, err := rpc.DialHTTP("tcp", reducer)
	if err != nil {
		log.Fatal("Connecting to Reducer: ", err)
	}
	defer c.Close()
	if err := c.Call("Reducer.Hello", &e{}, &e{}); err != nil {
		log.Fatal("Reducer.Hello: ", err)
	}
	var calls []*rpc.Call
	for {
		var w interface{}
		if err := c.Call("Reducer.GetWork", &e{}, &w); err != nil {
			if err.String() == ErrDone.String() {
				break
			}
			log.Fatal("Reducer.GetWork: ", err)
		}
		r, err := mFunc(w)
		if err != nil {
			log.Fatal("Mapper: ", err)
		}
		call := c.Go("Reducer.SendResult", &r, &e{}, nil)
		calls = append(calls, call)
	}
	for _, call := range calls {
		<-call.Done
	}
	if err := c.Call("Reducer.Goodbye", &e{}, &e{}); err != nil {
		log.Fatal("Reducer.Goodbye: ", err)
	}
	log.Print("Mapper exited cleanly; shutting down")
}
