package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/euforia/sql-raft/raft"
	"github.com/euforia/sql-raft/resource"
	"github.com/euforia/sql-raft/resource/pgsql"
)

var (
	listenAddr = flag.String("l", "127.0.0.1:54321", "Raft bind address")
	advertAddr = flag.String("a", "", "Raft advertise address")
	dataDir    = flag.String("p", "/tmp/", "Raft data dir")
	resrc      = flag.String("r", "dummy", "Backend resource")
	join       = flag.String("j", "", "Cluster node to join")
)

func init() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func printRaftStats(rl *raft.RaftLayer) {
	b, _ := json.MarshalIndent(rl.Raft.Stats(), "  ", "  ")
	log.Printf("\nState:\n  %s\n", b)
}

func main() {

	rl, err := raft.NewRaftLayer(true, filepath.Join(*dataDir, *listenAddr), *listenAddr, *advertAddr)
	if err != nil {
		log.Fatal(err)
	}

	rl.RegisterRpcService(rl)

	if *join != "" {
		if err := rl.Join(*join); err != nil {
			log.Fatal(err)
		}
	}

	printRaftStats(rl)

	var nd Resource
	log.Println("Initializing resource:", *resrc)

	switch *resrc {
	case "dummy":
		dfile, err := os.Getwd()
		if err != nil {
			dfile = "dummy-resource.sh"
		} else {
			dfile = filepath.Join(dfile, "dummy-resource.sh")
		}
		nd = resource.NewDummyResource(dfile, []string{"test"})

	case "postgres":
		nd = pgsql.NewDockerPostgresResource()

	default:
		log.Fatal("Unsupported resource:", *resrc)
	}

	stmgr := newStateMgr(rl, 5, nd)
	stmgr.start()
}
