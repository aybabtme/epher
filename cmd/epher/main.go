package main

import (
	"log"
	"os"

	"github.com/alecthomas/kingpin"
	"github.com/aybabtme/epher/cluster"
	"github.com/aybabtme/epher/service"
	"github.com/aybabtme/epher/store"
)

var (
	app = kingpin.New("epher", "A highly available, content addressable distributed blob storage.")

	node      = app.Command("node", "Join a cluster and become a storage node.")
	storage   = node.Flag("store", "Type of storage to use.").Required().Default("memory").Enum("memory")
	joinAddrs = node.Flag("addrs", "Addresses of some members of the cluster to join.").Required().Strings()

	blob     = app.Command("blob", "Manipulate blobs in an epher cluster.")
	blobPut  = blob.Command("put", "Put a blob in epher.")
	blobGet  = blob.Command("get", "Get a blob from epher.")
	blobInfo = blob.Command("info", "Info about a blob in epher.")
)

func main() {
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	case node.FullCommand():
		// join or form a cluster
		runNode((*joinAddrs)...)

	case blob.FullCommand():

	}
}

func runNode(addrs ...string) {
	sd, err := cluster.JoinLAN(addrs...)
	if err != nil {
		log.Fatal(err)
	}
	err = service.Start(sd, store.NewMemoryStore())
	if err != nil {
		log.Fatal(err)
	}
}
