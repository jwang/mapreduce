package mapreduce

import (
	"log"
	"os"
	"rpc"
	"sync"
)

const (
	Kind_Producer = iota + 1
	Kind_Mapper
	Kind_Reducer
)

type Ident struct {
	Kind int
	Addr string
}

type Empty struct{}

type master struct {
	Producer *Ident
	Mapper   []*Ident
	Reducer  *Ident
	mu       sync.RWMutex
}

func runMaster() {
	rpc.RegisterName("Master", &master{})
}

func (m *master) Hello(i *Ident, r *Empty) os.Error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch i.Kind {
	case Kind_Producer:
		if m.Producer != nil {
			return os.NewError("Producer already registered")
		}
		m.Producer = i
		log.Print("Hello Producer: ", i.Addr)
	case Kind_Mapper:
		m.Mapper = append(m.Mapper, i)
		log.Print("Hello Mapper: ", i.Addr)
	case Kind_Reducer:
		if m.Reducer != nil {
			return os.NewError("Reducer already registered")
		}
		m.Reducer = i
		log.Print("Hello Reducer: ", i.Addr)
	}
	return nil
}

func (m *master) Lookup(kind *int, addr *string) os.Error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	switch *kind {
	case Kind_Producer:
		if m.Producer == nil {
			return os.NewError("No Producer registered")
		}
		*addr = m.Producer.Addr
	case Kind_Reducer:
		if m.Reducer == nil {
			return os.NewError("No Reducer registered")
		}
		*addr = m.Reducer.Addr
	default:
		return os.NewError("Invalid Kind")
	}
	return nil
}

func masterHello(kind int) *rpc.Client {
	c, err := rpc.DialHTTP("tcp", *masterAddr)
	if err != nil {
		log.Fatal("Connecting to Reducer:", err)
	}
	ident := &Ident{
		Kind: kind,
		Addr: listenAddr.String(),
	}
	if err := c.Call("Master.Hello", ident, &Empty{}); err != nil {
		log.Fatal("Master.Hello:", err)
	}
	return c
}

func masterLookup(c *rpc.Client, kind int) (addr string) {
	err := c.Call("Master.Lookup", kind, &addr)
	if err != nil {
		log.Fatal("Master.Lookup:", err)
	}
	return
}
