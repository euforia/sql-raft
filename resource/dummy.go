package resource

import (
	"log"
	"os"
	"os/exec"
	"syscall"
)

type DummyResource struct {
	leader string

	cmd *exec.Cmd

	binPath string
	binArgs []string
}

func NewDummyResource(binPath string, args []string) *DummyResource {
	return &DummyResource{
		binPath: binPath,
		binArgs: args,
	}
}

func (nd *DummyResource) Metadata() map[string]interface{} {
	return map[string]interface{}{
		"bin_path": nd.binPath,
		"bin_args": nd.binArgs,
	}
}

func (nd *DummyResource) Start(leader ...string) error {
	if len(leader) > 0 {
		log.Printf("Starting as follower: leader='%s'\n", leader[0])
		nd.leader = leader[0]

		procArgs := append([]string{"follower"}, nd.binArgs...)
		return nd.startProcess(procArgs)
	}

	nd.leader = ""
	log.Println("Starting as leader.")

	procArgs := append([]string{"leader"}, nd.binArgs...)
	return nd.startProcess(procArgs)
}

func (nd *DummyResource) Demote(leader string) error {
	nd.Stop()
	return nd.Start(leader)
}

func (nd *DummyResource) Reassign(leader string) error {
	nd.Stop()
	return nd.Start(leader)
}

func (nd *DummyResource) Promote() error {
	nd.Stop()
	return nd.Start()
}

func (nd *DummyResource) Stop() error {

	log.Printf("[Stop] Stopping (pid=%d)\n", nd.cmd.Process.Pid)
	if err := nd.cmd.Process.Signal(syscall.SIGTERM); err != nil {

		log.Println("[Stop] Error from SIGTERM. Killing")
		if err = nd.cmd.Process.Kill(); err != nil {
			log.Println(err)
		}

	}
	//log.Printf("[Stop] Waiting (pid=%d)\n", nd.cmd.Process.Pid)
	log.Println("[Stop] Stopped:", nd.cmd.Wait())
	return nil
}

func (nd *DummyResource) startProcess(args []string) error {

	nd.cmd = exec.Command(nd.binPath, args...)
	nd.cmd.Stdout = os.Stdout
	nd.cmd.Stderr = os.Stderr

	err := nd.cmd.Start()
	if err == nil {
		log.Printf("Started (pid=%d)\n", nd.cmd.Process.Pid)
		// to catch the intial output???
		os.Stdout.Sync()
		os.Stderr.Sync()
	}

	return err

}
