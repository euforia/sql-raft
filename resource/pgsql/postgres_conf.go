package pgsql

import (
	"fmt"
	"path/filepath"
	"strings"
)

type postgresqlConf struct {
	WalLevel           string
	MaxWalSenders      int
	CheckpointSegments int
	WalKeepSegments    int
	HotStandby         bool
	SynchStandbyNames  []string //synchronous_standby_names
}

func defaultPostgresqlConf() postgresqlConf {
	return postgresqlConf{
		WalLevel:           "hot_standby",
		MaxWalSenders:      3,
		CheckpointSegments: 4,
		WalKeepSegments:    4,
		SynchStandbyNames:  []string{},
	}
}

func (pc *postgresqlConf) filename() string {
	return "postgresql.conf"
}

func (pc *postgresqlConf) commit(datadir string) error {
	cf := newKvConfigFile(filepath.Join(datadir, pc.filename()), " = ")
	err := cf.open()
	if err != nil {
		return err
	}

	cf.set("wal_level", pc.WalLevel)
	cf.set("max_wal_senders", fmt.Sprintf("%d", pc.MaxWalSenders))
	cf.set("wal_keep_segments", fmt.Sprintf("%d", pc.WalKeepSegments))

	if pc.HotStandby {
		cf.set("hot_standby", "on")
	}

	if len(pc.SynchStandbyNames) > 0 {
		cf.set("synchronous_standby_names", strings.Join(pc.SynchStandbyNames, ","))
	}

	return cf.commit()
}
