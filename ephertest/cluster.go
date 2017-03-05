package ephertest

import (
	"net"
	"sync"
	"testing"

	"github.com/aybabtme/epher/cluster"
)

func ServiceDiscovery(t *testing.T) cluster.Discovery {
	return &memDiscovery{
		members: make(map[cluster.Node]struct{}),
	}
}

type memDiscovery struct {
	membersMu sync.Mutex
	members   map[cluster.Node]struct{}
}

func (mem *memDiscovery) Discover(addr ...string) (cluster.RemoteCluster, error) {
	return &memRemoteCluster{global: mem}, nil
}

func (mem *memDiscovery) join(self cluster.Node) error {
	mem.membersMu.Lock()
	mem.members[self] = struct{}{}
	mem.membersMu.Unlock()
	return nil
}

func (mem *memDiscovery) leave(self cluster.Node) error {
	mem.membersMu.Lock()
	delete(mem.members, self)
	mem.membersMu.Unlock()
	return nil
}

func (mem *memDiscovery) memberList() []cluster.Node {
	mem.membersMu.Lock()
	dup := make([]cluster.Node, len(mem.members))
	for nd := range mem.members {
		dup = append(dup, nd)
	}
	mem.membersMu.Unlock()
	return dup
}

type memRemoteCluster struct {
	global *memDiscovery
}

func (mem *memRemoteCluster) Members() []cluster.Node {
	return mem.global.memberList()
}

func (mem *memRemoteCluster) Join(cb func(ip string) (net.Addr, error)) (cluster.Cluster, error) {

	addr, err := cb("127.0.0.1")
	if err != nil {
		return nil, err
	}

	self := cluster.Node{
		Addr: addr.String(),
	}

	if err := mem.global.join(self); err != nil {
		return nil, err
	}
	return &memCluster{global: mem.global, self: self}, nil
}

type memCluster struct {
	global *memDiscovery
	self   cluster.Node
}

func (mem *memCluster) Self() cluster.Node {
	return mem.self
}

func (mem *memCluster) Members() []cluster.Node {
	return mem.global.memberList()
}

func (mem *memCluster) Leave() error {
	return mem.global.leave(mem.self)
}
