package pgsql

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

// key value based config
type kvConfigFile struct {
	cfgfile string
	delim   string
	kv      map[string]string
}

func newKvConfigFile(cfgfile string, delim string) *kvConfigFile {
	return &kvConfigFile{
		cfgfile: cfgfile,
		kv:      map[string]string{},
		delim:   delim,
	}
}

func (cf *kvConfigFile) open() error {

	_, err := os.Stat(cf.cfgfile)
	// new file
	if os.IsNotExist(err) {
		return nil
	}

	b, err := ioutil.ReadFile(cf.cfgfile)
	if err != nil {
		return err
	}

	for _, l := range strings.Split(string(b), "\n") {
		cl := strings.TrimSpace(l)
		if strings.HasPrefix(cl, "#") || cl == "" {
			continue
		}

		kv := strings.Split(cl, cf.delim)
		if len(kv) != 2 {
			log.Println(kv)
			return fmt.Errorf("invalid config: %s", l)
		}

		cf.kv[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])

	}

	return nil
}

func (cf *kvConfigFile) set(key, value string) {
	cf.kv[key] = value
}

func (cf *kvConfigFile) commit() error {

	s := make([]string, len(cf.kv))
	i := 0
	for k, v := range cf.kv {
		s[i] = fmt.Sprintf("%s%s%s", k, cf.delim, v)
		i++
	}

	if s[len(s)-1] != "" {
		s = append(s, "")
	}

	cnts := strings.Join(s, "\n")

	return ioutil.WriteFile(cf.cfgfile, []byte(cnts), 0777)

}
