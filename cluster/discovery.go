package cluster

import "github.com/hashicorp/memberlist"

type Discovery interface {
	Members() []*memberlist.Node
	Self() *memberlist.Node
}

type gossipDiscovery struct {
	list *memberlist.Memberlist
}

func JoinLAN(addrs ...string) (Discovery, error) {
	list, err := memberlist.Create(memberlist.DefaultLANConfig())
	if err != nil {
		return nil, err
	}
	_, err = list.Join(addrs)
	if err != nil {
		return nil, err
	}
	return &gossipDiscovery{list: list}, nil
}

func (gd *gossipDiscovery) Members() []*memberlist.Node { return gd.list.Members() }
func (gd *gossipDiscovery) Self() *memberlist.Node      { return gd.list.LocalNode() }
