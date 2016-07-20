package pgsql

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	pgDataDir = "/var/lib/postgresql/data/"
)

type pgConf struct {
	version string
	user    string
	dir     string
	port    int
	bins    map[string]string
}

func newPgConf(version string) *pgConf {
	p := &pgConf{
		port:    5432,
		dir:     pgDataDir,
		user:    "postgres",
		bins:    map[string]string{},
		version: version,
	}

	baseBinDir := filepath.Join("/usr/lib/postgresql", version, "bin")
	p.bins = map[string]string{}
	for _, v := range []string{"pg_ctl", "postgres", "pg_basebackup"} {
		p.bins[v] = filepath.Join(baseBinDir, v)
	}

	return p
}

func (p *pgConf) promoteCmd() string {
	return fmt.Sprintf("%s promote -D %s", p.bins["pg_ctl"], p.dir)
}

func (p *pgConf) stopCmd() string {
	return fmt.Sprintf("%s stop -D %s", p.bins["pg_ctl"], p.dir)
}

func (p *pgConf) initCmd() string {
	return fmt.Sprintf("%s init -D %s", p.bins["pg_ctl"], p.dir)
}

func (p *pgConf) startCmd() string {
	return fmt.Sprintf("%s -D %s", p.bins["postgres"], p.dir)
}

func (p *pgConf) backupFromCmd(host string, port int) string {
	return fmt.Sprintf("%s -R -D %s --host=%s --port=%d -X stream -U %s",
		p.bins["pg_basebackup"], p.dir, host, port, p.user)
}

type DockerPostgresResource struct {
	leader string // master db

	pgconf *pgConf

	cmd *exec.Cmd
	mu  sync.Mutex
}

func NewDockerPostgresResource(version string) *DockerPostgresResource {
	// TODO: autodetermine and check for > 9.3

	d := &DockerPostgresResource{
		pgconf: newPgConf(version),
	}

	return d
}

func (nd *DockerPostgresResource) initResource() error {

	if _, err := os.Stat(nd.pgPath("PG_VERSION")); err == nil {
		return nil
	}

	os.MkdirAll(nd.pgconf.dir, 0777)
	os.Chown(nd.pgconf.dir, 999, 999)

	cmd := exec.Command("su", nd.pgconf.user, "-c", nd.pgconf.initCmd())
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

	log.Println("Starting postgres...")
	nd.cmd = exec.Command("su", nd.pgconf.user, "-c", nd.pgconf.startCmd())
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
// remote host and remote port
func (nd *DockerPostgresResource) syncFromLeader(host string, port int) (*exec.Cmd, error) {
	chown(nd.pgconf.dir, nd.pgconf.user)
	os.Chmod(nd.pgconf.dir, 0700)

	cmd := exec.Command("su", nd.pgconf.user, "-c", nd.pgconf.backupFromCmd(host, port))
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
	if _, err := os.Stat(nd.pgPath("PG_VERSION")); err != nil {
		// temporarily use local port configuration
		if cmd, err := nd.syncFromLeader(host, nd.pgconf.port); err == nil {
			if err = cmd.Wait(); err != nil {
				return err
			}
		}
	}

	// set recovery.conf with required params
	rc := defaultRecoveryConf()
	rc.ReplUser = nd.pgconf.user
	rc.Primary = host

	if err := rc.commit(nd.pgconf.dir); err != nil {
		return err
	}

	return chown(nd.pgPath(rc.filename()), nd.pgconf.user)
}

func (nd *DockerPostgresResource) configureLeader() error {
	log.Println("Starting as leader.")

	nd.leader = ""
	// remove recovery.conf in case it's left over after promotion
	func() {
		rc := recoveryConf{}
		os.Remove(nd.pgPath(rc.filename()))
	}()

	var err error
	if err = nd.initResource(); err == nil {
		pc := defaultPostgresqlConf()
		err = pc.commit(nd.pgconf.dir)
	}

	return err
}

// add user and commit to disk
func (nd *DockerPostgresResource) setReplicationUser(user, address string) error {
	hba := newPgHbaConf()
	if err := hba.load(nd.pgconf.dir); err != nil {
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
	return hba.commit(nd.pgconf.dir)
}

func (nd *DockerPostgresResource) Start(leader ...string) error {
	var err error
	if len(leader) > 0 {
		err = nd.configureFollower(leader[0])
	} else {
		err = nd.configureLeader()
	}

	if err == nil {
		if err = nd.setReplicationUser(nd.pgconf.user, "172.17.0.0/24"); err == nil {
			err = nd.startResource()
		}
	}

	return err
}

func (nd *DockerPostgresResource) Stop() error {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	// Stop via pg_ctl
	cmd := exec.Command("su", nd.pgconf.user, "-c", nd.pgconf.stopCmd())
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
	cmd := exec.Command("su", dp.pgconf.user, "-c", dp.pgconf.promoteCmd())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return err
	}

	return cmd.Wait()
}

// path relative to pgdata
func (nd *DockerPostgresResource) pgPath(filename string) string {
	return filepath.Join(nd.pgconf.dir, filename)
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
