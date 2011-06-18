package main

import (
	"flag"
	"fmt"
	"github.com/nf/mapreduce"
	"os"
	"math"
)

var n = flag.Int("n", 5000, "Number of series to calculate")

func main() {
	flag.Parse()
	mapreduce.Run(produce(*n), mapper, reducer)
}

func produce(n int) mapreduce.Producer {
	return func(work chan<- interface{}) os.Error {
		for k := 0; k <= n; k++ {
			work <- float64(k)
		}
		close(work)
		return nil
	}
}

func mapper(work interface{}) (result interface{}, err os.Error) {
	k, ok := work.(float64)
	if !ok {
		err = os.NewError("unexpected type of work")
		return
	}
	result = 4 * math.Pow(-1, k) / (2*k + 1)
	return
}

func reducer(result <-chan interface{}) os.Error {
	var f float64
	for r := range result {
		v, ok := r.(float64)
		if !ok {
			return os.NewError("unexpected type of result")
		}
		f += v
	}
	fmt.Println("Result: ", f)
	return nil
}
