package pgsql

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type pgHbaConf struct {
	m map[string]Acl
}

func newPgHbaConf() *pgHbaConf {
	return &pgHbaConf{map[string]Acl{}}
}

func (p *pgHbaConf) filename() string {
	return "pg_hba.conf"
}

func (p *pgHbaConf) AddAcl(acl Acl) {
	p.m[acl.hashKey()] = acl
}

func (p *pgHbaConf) commit(datadir string) error {

	out := ""
	for _, v := range p.m {
		out += fmt.Sprintf("%s %s %s %s %s\n", v.Type, v.Database, v.User, v.Address, v.Method)
	}

	if out == "" {
		return fmt.Errorf("nothing to write. output empty")
	}

	return ioutil.WriteFile(filepath.Join(datadir, p.filename()), []byte(out), 0777)
}

func (p *pgHbaConf) load(datadir string) error {
	cfile := filepath.Join(datadir, p.filename())
	_, err := os.Stat(cfile)
	// new config file
	if os.IsNotExist(err) {
		return nil
	}

	b, err := ioutil.ReadFile(cfile)
	if err != nil {
		return err
	}

	contents := string(b)
	for _, l := range strings.Split(contents, "\n") {
		cl := strings.TrimSpace(l)
		if strings.HasPrefix(cl, "#") || cl == "" {
			continue
		}

		parts := strings.Split(cl, " ")
		tp := []string{}
		for _, t := range parts {
			tt := strings.TrimSpace(t)
			if tt == "" {
				continue
			}

			tp = append(tp, tt)
		}

		var acl Acl
		switch len(tp) {
		case 5:
			acl = Acl{
				Type:     tp[0],
				Database: tp[1],
				User:     tp[2],
				Address:  tp[3],
				Method:   tp[4],
			}

		case 4:
			// no address
			acl = Acl{
				Type:     tp[0],
				Database: tp[1],
				User:     tp[2],
				Method:   tp[3],
			}

		default:
			return fmt.Errorf("invalid line: '%s'", cl)
		}

		p.AddAcl(acl)

	}

	return nil
}

type Acl struct {
	Type     string
	Database string
	User     string
	Address  string
	Method   string
}

func (a *Acl) hashKey() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s",
		a.Type, a.Database, a.User, a.Address, a.Method)
}

func DefaultAcl() Acl {
	return Acl{Type: "host", Method: "trust", Address: "0.0.0.0/0"}
}
