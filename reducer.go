package mapreduce

import (
	"log"
	"os"
	"rpc"
)

func runReducer(rFunc Reducer) {
	masterHello(Kind_Reducer).Close()
	r := &reducer{result: make(chan interface{})}
	rpc.RegisterName("Reducer", r)
	go func() {
		err := rFunc(r.result)
		if err != nil {
			log.Fatal("Reducer error:", err)
		}
		log.Print("Reducer exited cleanly; shutting down")
		os.Exit(0)
	}()
}

type reducer struct {
	result chan interface{}
}

func (r *reducer) SendResult(result *interface{}, reply *Empty) os.Error {
	r.result <- *result
	return nil
}

func (r *reducer) Done(args *Empty, reply *Empty) os.Error {
	close(r.result)
	return nil
}
