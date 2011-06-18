package mapreduce

import (
	"flag"
	"http"
	"log"
	"os"
	"rpc"
)

var ErrDone = os.NewError("End of stream")

type Mapper func(work interface{}) (result interface{}, err os.Error)

type Reducer func(work chan<- interface{}, result <-chan interface{}) os.Error

var (
	httpAddr    = flag.String("http", ":0", "HTTP server listen address")
	reducerAddr = flag.String("reducer", "", "reducer server URL")
	isMapper    = flag.Bool("map", false, "act as a mapper")
)

func Run(r Reducer, m Mapper) {
	flag.Parse()

	if *isMapper {
		if *reducerAddr == "" {
			flag.Usage()
			os.Exit(2)
		}
		log.Println("Acting as a Mapper")
		runMapper(*reducerAddr, m)
		return
	}

	log.Println("Acting as Reducer")
	rpc.RegisterName("Reducer", newReducer(r))
	rpc.HandleHTTP()
	err := http.ListenAndServe(*httpAddr, nil)
	if err != nil {
		log.Fatal(err)
	}
}
