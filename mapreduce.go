package mapreduce

import (
	"flag"
	"http"
	"log"
	"net"
	"os"
	"rpc"
)

var ErrDone = os.NewError("End of stream")

type Producer func(work chan<- interface{}) os.Error

type Mapper func(work interface{}) (result interface{}, err os.Error)

type Reducer func(result <-chan interface{}) os.Error

var (
	httpAddr   = flag.String("http", ":0", "HTTP server listen address")
	masterAddr = flag.String("master", "", "master server URL")
	isProducer = flag.Bool("produce", false, "act as a producer")
	isMapper   = flag.Bool("map", false, "act as a mapper")
	isReducer  = flag.Bool("reduce", false, "act as a reducer")
)

var listenAddr net.Addr

func Run(p Producer, m Mapper, r Reducer) {
	l, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		log.Fatal("Listen:", err)
	}
	listenAddr = l.Addr()
	log.Println("Listening on", listenAddr)
	switch {
	case *isProducer:
		log.Println("Acting as Producer")
		runProducer(p)
	case *isMapper:
		log.Println("Acting as a Mapper")
		runMapper(m)
	case *isReducer:
		log.Println("Acting as Reducer")
		runReducer(r)
	default:
		log.Println("Acting as Master")
		runMaster()
	}
	log.Println("Staring HTTP-RPC server")
	rpc.HandleHTTP()
	err = http.Serve(l, nil)
	if err != nil {
		log.Fatal(err)
	}
}
