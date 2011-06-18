package mapreduce

import (
	"log"
	"os"
	"rpc"
	"time"
)

func runProducer(pFunc Producer) {
	masterHello(Kind_Producer).Close()
	p := &producer{work: make(chan interface{})}
	rpc.RegisterName("Producer", p)
	go func() {
		err := pFunc(p.work)
		if err != nil {
			log.Fatal("Producer error:", err)
		}
		log.Print("Producer exited cleanly")
		masterGoodbye(Kind_Producer)
	}()
}

type producer struct {
	work chan interface{}
}

func (p *producer) GetWork(args *Empty, reply *interface{}) os.Error {
	if r, ok := <-p.work; ok {
		*reply = r
		return nil
	}
	return ErrDone
}

func (p *producer) Shutdown(args *Empty, reply *Empty) os.Error {
	go func() {
		time.Sleep(1e9)
		log.Print("Shutting down")
		os.Exit(0)
	}()
	return nil
}
