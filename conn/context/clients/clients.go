package clients

import (
	"sync"
)

// Clients manage client collections
// Ensure concurrent secure access
type Clients struct {
	lock    sync.RWMutex
	clients map[string]interface{}
}

// NewClients create a new object
func NewClients() *Clients {
	return &Clients{
		clients: make(map[string]interface{}),
	}
}

// Register add client to clients
func (cs *Clients) Register(key string, cli interface{}) {
	cs.lock.Lock()
	cs.clients[key] = cli
	cs.lock.Unlock()
}

// Deregister delete client in clients
func (cs *Clients) Deregister(key string) {
	cs.lock.Lock()
	delete(cs.clients, key)
	cs.lock.Unlock()
}

// Scan calls f sequentially for each key and value present in the map.
func (cs *Clients) Scan(handler func(client interface{}) error) {
	clients := cs.Clone()
	for _, cli := range clients {
		if err := handler(cli); err != nil {
			return
		}
	}
}

// Clone the entire data set
func (cs *Clients) Clone() map[string]interface{} {
	cs.lock.RLock()
	csc := make(map[string]interface{}, len(cs.clients))
	for k, v := range cs.clients {
		csc[k] = v
	}
	cs.lock.RUnlock()
	return csc
}

// Exist if key is exist return true,otherwise return false
func (cs *Clients) Exist(key string) bool {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	if _, ok := cs.clients[key]; ok {
		return true
	}
	return false
}

// Get if key is exist return client ,otherwise return nil
func (cs *Clients) Get(key string) interface{} {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	if v, ok := cs.clients[key]; ok {
		return v
	}
	return nil
}

func (cs *Clients) Length() int {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	return len(cs.clients)
}
