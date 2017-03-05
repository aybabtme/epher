package cluster

import (
	"net"

	"github.com/hashicorp/memberlist"
)

type Node struct {
	Addr string
}

type Discovery interface {
	Discover(addr ...string) (RemoteCluster, error)
}

type RemoteCluster interface {
	Members() []Node
	Join(cb func(ip string) (net.Addr, error)) (Cluster, error)
}

type Cluster interface {
	Self() Node
	Members() []Node
	Leave() error
	// additional stuff
}

type gossipDiscovery struct {
	// nothing
}

func (gd *gossipDiscovery) Discover(addrs ...string) (RemoteCluster, error) {
	cfg := memberlist.DefaultLANConfig()
	list, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}
	_, err = list.Join(addrs)
	if err != nil {
		return nil, err
	}

	return &gossipCluster{list: list}, nil
}

type gossipCluster struct {
	list *memberlist.Memberlist
}

func (gd *gossipCluster) Members() []Node {

	// gd.list.SendToTCP

	// TODO: discover what ports people are listening on

	return nil
}

func (gd *gossipCluster) Join(cb func(ip string) (net.Addr, error)) (Cluster, error) {
	addr, err := cb(gd.list.LocalNode().Addr.String())
	if err != nil {
		return nil, err
	}

	// TODO: announce to people that we are listening on "addr"
	_ = addr

	return nil, nil
}
