package pgsql

import (
	"fmt"
	//"log"
	"path/filepath"
	"strconv"
	"strings"
)

type recoveryConf struct {
	// these to are required for the raft portion
	TriggerFile            string
	RecoveryTargetTimeline string

	Primary     string
	Port        int
	ReplUser    string
	ReplPass    string
	StandbyMode bool
}

func defaultRecoveryConf() recoveryConf {
	return recoveryConf{
		TriggerFile:            "'/tmp/postgres-master-trigger'",
		RecoveryTargetTimeline: "latest",
		Port:        5432,
		StandbyMode: true,
	}
}

func (rc *recoveryConf) filename() string {
	return "recovery.conf"
}

func (rc *recoveryConf) load(datadir string) error {
	cf := newKvConfigFile(filepath.Join(datadir, rc.filename()), " = ")
	err := cf.open()
	if err != nil {
		return err
	}

	for k, v := range cf.kv {
		switch k {
		case "trigger_file":
			rc.TriggerFile = v
		case "recovery_target_timeline":
			rc.RecoveryTargetTimeline = v
		case "primary_conninfo":

			parts := strings.Split(v, " ")
			for _, p := range parts {
				kv := strings.Split(strings.TrimSpace(p), "=")
				switch kv[0] {
				case "host":
					rc.Primary = kv[1]

				case "port":
					port, _ := strconv.ParseInt(kv[1], 10, 32)
					rc.Port = int(port)

				case "user":
					rc.ReplUser = kv[1]

				case "password":
					rc.ReplPass = kv[1]
				}
			}

		case "standby_mode":
			if v == "on" {
				rc.StandbyMode = true
			} else {
				rc.StandbyMode = false
			}
		}
	}

	return nil
}
func (rc *recoveryConf) buildPrimaryConnInfo() string {
	s := fmt.Sprintf(`'host=%s port=%d`, rc.Primary, rc.Port)

	if rc.ReplUser != "" {
		s += " user=" + rc.ReplUser
		if rc.ReplPass != "" {
			s += " password=" + rc.ReplPass
		}
	}
	s += `'`
	return s
}

func (rc *recoveryConf) commit(datadir string) error {
	// get existing kv's
	cf := newKvConfigFile(filepath.Join(datadir, rc.filename()), " = ")
	err := cf.open()
	if err != nil {
		return err
	}

	if rc.RecoveryTargetTimeline != "" {
		cf.set("recovery_target_timeline", rc.RecoveryTargetTimeline)
	}
	if rc.TriggerFile != "" {
		cf.set("trigger_file", rc.TriggerFile)
	}

	if rc.Primary != "" {
		s := rc.buildPrimaryConnInfo()
		cf.set("primary_conninfo", s)
	}

	if rc.StandbyMode {
		cf.set("standby_mode", "on")
	}

	//log.Println(cf.kv)

	return cf.commit()

}
