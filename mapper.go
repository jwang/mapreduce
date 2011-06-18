package mapreduce

import (
	"log"
	"os"
	"rpc"
)

func runMapper(mFunc Mapper) {
	c := masterHello(Kind_Mapper)
	defer c.Close()
	p, r := masterLookup(c, Kind_Producer), masterLookup(c, Kind_Reducer)
	go mapLoop(p, r, mFunc)
}

func mapLoop(producer, reducer string, mFunc Mapper) {
	cp, err := rpc.DialHTTP("tcp", producer)
	if err != nil {
		log.Fatal("Connecting to Producer:", err)
	}
	defer cp.Close()
	cr, err := rpc.DialHTTP("tcp", reducer)
	if err != nil {
		log.Fatal("Connecting to Reducer:", err)
	}
	defer cr.Close()
	var calls []*rpc.Call
	for {
		var w interface{}
		if err := cp.Call("Producer.GetWork", &Empty{}, &w); err != nil {
			if err.String() == ErrDone.String() {
				break
			}
			log.Fatal("Producer.GetWork:", err)
		}
		r, err := mFunc(w)
		if err != nil {
			log.Fatal("Mapper error:", err)
		}
		c := cr.Go("Reducer.SendResult", &r, &Empty{}, nil)
		calls = append(calls, c)
	}
	for _, c := range calls {
		<-c.Done
	}
	masterGoodbye(Kind_Mapper)
	log.Print("Mapper exited cleanly; shutting down")
	os.Exit(0)
}
