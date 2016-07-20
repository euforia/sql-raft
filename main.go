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
	listenAddr = flag.String("b", "0.0.0.0:54321", "Raft bind address")
	advertAddr = flag.String("a", "", "Raft advertise address")
	dataDir    = flag.String("p", "/tmp/", "Raft data dir")
	join       = flag.String("j", "", "Cluster node to join")

	resrc = flag.String("r", "dummy", "Backend resource [ dummy | postgres ]")
	// default postgres version
	rsrcVersion = flag.String("r-version", "9.5", "Version of resource")
)

func init() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func printRaftStats(rl *raft.RaftLayer) {
	b, _ := json.MarshalIndent(rl.Raft.Stats(), "  ", "  ")
	log.Printf("\nState:\n  %s\n", b)
}

func initResource() Resource {
	log.Println("Initializing resource:", *resrc)

	var nd Resource

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
		nd = pgsql.NewDockerPostgresResource(*rsrcVersion)

	default:
		log.Fatal("Unsupported resource:", *resrc)
	}

	return nd
}

func main() {
	ddir := filepath.Join(*dataDir, *listenAddr)

	rl, err := raft.NewRaftLayer(true, ddir, *listenAddr, *advertAddr)
	if err != nil {
		log.Fatal(err)
	}
	//rl.RegisterRpcService(rl)
	if *join != "" {
		if err := rl.Join(*join); err != nil {
			log.Fatal(err)
		}
	}

	printRaftStats(rl)

	nd := initResource()
	stmgr := newStateMgr(rl, 5, nd)
	stmgr.start()
}
