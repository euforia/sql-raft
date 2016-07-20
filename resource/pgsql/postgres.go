package pgsql

import (
	"fmt"
	"log"
	"os"
	"os/user"
	"sync"
	//"io/ioutil"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	//"syscall"
	"time"
)

const (
	pgDataDir = "/var/lib/postgresql/data/"
	//pgDataDir = "/etc/postgresql/"
)

type DockerPostgresResource struct {
	leader string

	pgctlPath string
	pgBin     string
	pgBkpBin  string
	pgUser    string
	pgDataDir string

	cmd *exec.Cmd
	mu  sync.Mutex
}

func NewDockerPostgresResource() *DockerPostgresResource {
	// TODO: autodetermine and check for > 9.3
	d := &DockerPostgresResource{
		pgctlPath: "/usr/lib/postgresql/9.5/bin/pg_ctl",
		pgBin:     "/usr/lib/postgresql/9.5/bin/postgres",
		pgBkpBin:  "/usr/lib/postgresql/9.5/bin/pg_basebackup",
		pgUser:    "postgres",
		pgDataDir: pgDataDir,
	}

	return d
}

func (nd *DockerPostgresResource) initResource() error {

	if _, err := os.Stat(filepath.Join(nd.pgDataDir, "PG_VERSION")); err == nil {
		return nil
	}

	os.MkdirAll(nd.pgDataDir, 0777)
	os.Chown(nd.pgDataDir, 999, 999)

	cmd := exec.Command("su", nd.pgUser, "-c", nd.pgctlPath+" init -D "+nd.pgDataDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Wait()
}

func (nd *DockerPostgresResource) startResource() error {
	if nd.cmd != nil {
		return fmt.Errorf("already running: pid=%d", nd.cmd.Process.Pid)
	}

	log.Println("Launching postgres...")
	nd.cmd = exec.Command("su", nd.pgUser, "-c", nd.pgBin+" -D "+nd.pgDataDir)
	nd.cmd.Stdout = os.Stdout
	nd.cmd.Stderr = os.Stderr
	if err := nd.cmd.Start(); err != nil {
		return err
	}

	//return cmd.Wait()
	return nil
}

func (nd *DockerPostgresResource) waitForMaster(retries, interval int) error {

	retryInterval := time.Duration(interval) * time.Second
	tkr := time.NewTicker(retryInterval)

	var (
		cr  = 0 // curr retries
		err error
	)

	for cr < retries {
		<-tkr.C
		// TODO: check master for availability
		cr++
	}

	return err
}

// sync from leader and get recovery file.
func (nd *DockerPostgresResource) syncFromLeader(host string, port int) (*exec.Cmd, error) {
	chown(nd.pgDataDir, nd.pgUser)
	os.Chmod(nd.pgDataDir, 0700)

	cs := fmt.Sprintf("%s -R -D %s --host=%s --port=%d -X stream -U %s",
		nd.pgBkpBin, nd.pgDataDir, host, port, nd.pgUser)

	cmd := exec.Command("su", nd.pgUser, "-c", cs)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()

	return cmd, err
}

func (nd *DockerPostgresResource) configureFollower(leader string) error {
	log.Printf("Starting follower: leader='%s'\n", leader)

	nd.leader = leader

	if err := nd.waitForMaster(2, 3); err != nil {
		return fmt.Errorf("failed to contact master: %s", err.Error())
	}

	host := strings.Split(nd.leader, ":")[0]
	// sync if data dir not init'd
	if _, err := os.Stat(filepath.Join(nd.pgDataDir, "PG_VERSION")); err != nil {

		if cmd, err := nd.syncFromLeader(host, 5432); err == nil {
			if err = cmd.Wait(); err != nil {
				return err
			}
		}
	}

	// set recovery.conf with required params
	rc := defaultRecoveryConf()
	rc.ReplUser = nd.pgUser
	rc.Primary = host

	if err := rc.commit(nd.pgDataDir); err != nil {
		return err
	}

	return chown(filepath.Join(nd.pgDataDir, rc.filename()), nd.pgUser)
}

func (nd *DockerPostgresResource) configureLeader() error {
	log.Println("Starting as leader.")

	nd.leader = ""
	// remove recovery.conf in case it's left over after promotion
	func() {
		rc := recoveryConf{}
		os.Remove(filepath.Join(nd.pgDataDir, rc.filename()))
	}()

	var err error
	if err = nd.initResource(); err == nil {
		pc := defaultPostgresqlConf()
		err = pc.commit(nd.pgDataDir)
	}

	return err
}

// add user and commit to disk
func (nd *DockerPostgresResource) setReplicationUser(user, address string) error {
	hba := newPgHbaConf()
	if err := hba.load(nd.pgDataDir); err != nil {
		return err
	}

	hba.AddAcl(Acl{
		Type:     "host",
		Method:   "trust",
		Database: "replication",
		User:     user,
		Address:  address,
	})

	// add acl
	return hba.commit(nd.pgDataDir)
}

func (nd *DockerPostgresResource) Start(leader ...string) error {
	var err error
	if len(leader) > 0 {
		err = nd.configureFollower(leader[0])
	} else {
		err = nd.configureLeader()
	}

	if err == nil {
		if err = nd.setReplicationUser(nd.pgUser, "172.17.0.0/24"); err == nil {
			err = nd.startResource()
		}
	}

	return err
}

func (nd *DockerPostgresResource) Stop() error {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	// Stop via pg_ctl
	cmd := exec.Command("su", nd.pgUser, "-c", nd.pgctlPath+" stop -D "+nd.pgDataDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return err
	}
	// Wait for pg_ctl
	if err = cmd.Wait(); err != nil {
		return err
	}
	/*
		// Signal our process to terminate
		if err = nd.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			// Kill if termination errors
			log.Printf("[WARNING] Killing: pid=%d %s\n", nd.cmd.Process.Pid, nd.cmd.Process.Kill())
		}
	*/
	log.Println("Waiting for postgres to stop...")
	// block - wait main postgres process to finish
	nd.cmd.Wait()
	// Clear out our resources
	nd.cmd = nil

	return nil
}

func (dp *DockerPostgresResource) Demote(leader string) error {
	if err := dp.Stop(); err != nil {
		return err
	}

	return dp.Start(leader)
}

func (dp *DockerPostgresResource) Reassign(leader string) error {

	if err := dp.Stop(); err != nil {
		return err
	}

	return dp.Start(leader)

}

func (dp *DockerPostgresResource) Promote() error {
	cmdstr := fmt.Sprintf("%s promote -D %s", dp.pgctlPath, dp.pgDataDir)

	cmd := exec.Command("su", dp.pgUser, "-c", cmdstr)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return err
	}

	return cmd.Wait()
}

func chown(filename, username string) error {
	usr, err := user.Lookup(username)
	if err == nil {
		gid, _ := strconv.ParseInt(usr.Gid, 10, 64)
		uid, _ := strconv.ParseInt(usr.Uid, 10, 64)
		//log.Printf("Setting permissions: uid=%d gid=%d\n", uid, gid)
		return os.Chown(filename, int(uid), int(gid))
	}
	return err
}
