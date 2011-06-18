package mapreduce

import (
	"log"
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
		err := cp.Call("Producer.GetWork", &Empty{}, &w)
		if err == ErrDone {
			break
		} else if err != nil {
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
	log.Print("Mapper exited cleanly")
}
