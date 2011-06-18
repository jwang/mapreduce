package mapreduce

import (
	"log"
	"os"
	"rpc"
	"sync"
	"time"
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
	Done     bool
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

func (m *master) Goodbye(i *Ident, r *Empty) os.Error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch i.Kind {
	case Kind_Producer:
		m.Done = true
		log.Print("Goodbye Producer: ", i.Addr)
	case Kind_Mapper:
		mm := m.Mapper
		for j := range mm {
			if mm[j].Addr == i.Addr {
				// remove mm[j]
				k := len(mm) - 1
				mm[j] = mm[k]
				m.Mapper = mm[:k]
			}
		}
		log.Print("Goodbye Mapper: ", i.Addr)
		if len(m.Mapper) == 0 && m.Done {
			// shut down Reducer and everything else
			go m.shutdown()
		}
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

func (m *master) shutdown() {
	m.mu.RLock()
	producer, reducer := m.Producer.Addr, m.Reducer.Addr
	m.mu.RUnlock()

	log.Print("Shutting down")
	if m.Producer != nil {
		c, err := rpc.DialHTTP("tcp", producer)
		if err != nil {
			log.Print("Dialling Producer: ", err)
			goto next
		}
		err = c.Call("Producer.Shutdown", &Empty{}, &Empty{})
		if err != nil {
			log.Print("Producer.Shutdown: ", err)
		}
	}
next:
	if m.Reducer != nil {
		c, err := rpc.DialHTTP("tcp", reducer)
		if err != nil {
			log.Print("Dialling Reducer: ", err)
			goto done
		}
		err = c.Call("Reducer.Done", &Empty{}, &Empty{})
		if err != nil {
			log.Print("Reducer.Done: ", err)
		}
	}
done:
	time.Sleep(1e9)
	log.Print("Exiting")
	os.Exit(0)
}


// Helper functions for communicating with the master.

func masterClient() *rpc.Client {
	c, err := rpc.DialHTTP("tcp", *masterAddr)
	if err != nil {
		log.Fatal("Connecting to Reducer:", err)
	}
	return c
}

func masterMsg(c *rpc.Client, msg string, kind int) {
	ident := &Ident{
		Kind: kind,
		Addr: listenAddr.String(),
	}
	if err := c.Call("Master."+msg, ident, &Empty{}); err != nil {
		log.Fatal("Master."+msg+": ", err)
	}
}

func masterHello(kind int) *rpc.Client {
	c := masterClient()
	masterMsg(c, "Hello", kind)
	return c
}

func masterGoodbye(kind int) {
	c := masterClient()
	defer c.Close()
	masterMsg(c, "Goodbye", kind)
}

func masterLookup(c *rpc.Client, kind int) (addr string) {
	err := c.Call("Master.Lookup", kind, &addr)
	if err != nil {
		log.Fatal("Master.Lookup: ", err)
	}
	return
}
