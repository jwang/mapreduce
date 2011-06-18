package mapreduce

import (
	"log"
	"os"
	"rpc"
)

func runProducer(pFunc Producer) {
	masterHello(Kind_Producer)
	p := &producer{work: make(chan interface{})}
	go func() {
		err := pFunc(p.work)
		if err != nil {
			log.Fatal("Producer error:", err)
		}
		log.Print("Producer exited cleanly")
	}()
	rpc.RegisterName("Producer", p)
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
