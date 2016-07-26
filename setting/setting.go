package setting

import (
	"io/ioutil"

	log "github.com/Sirupsen/logrus"
	"github.com/ghodss/yaml"
)

func init() {
	if err := InitConf("conf/global.yaml", "conf/runtime.yaml"); err != nil {
		log.Errorf("Read config error: %v", err.Error())
		return
	}
}

var (
	Global  *GlobalConf
	RunTime *RunTimeConf
)

type GlobalConf struct {
	AppName     string
	Usage       string
	Version     string
	Author      string
	Email       string
	RuntimePath string
}

type RunTimeConf struct {
	Run         *Run
	Http        *Http
	Sqldatabase *Sqldatabase
	Kvdatabase  *Kvdatabase
}

type Run struct {
	RunMode string
	LogPath string
}

type Sqldatabase struct {
	Driver   string
	Username string
	Password string
	Protocol string
	Host     string
	Port     string
	Schema   string
}

type Kvdatabase struct {
	Driver    string
	Username  string
	Password  string
	Protocol  string
	Host      string
	Port      string
	Schema    string
	Partition int64
}

type Http struct {
	ListenMode    string
	HttpsCertFile string
	HttpsKeyFile  string
}

func InitConf(globalFilePath string, runtimeFilePath string) error {

	globalFile, err := ioutil.ReadFile(globalFilePath)
	if err != nil {
		return err
	}
	Global = &GlobalConf{}
	err = yaml.Unmarshal([]byte(globalFile), &Global)
	if err != nil {
		return err
	}

	runtimeFile, err := ioutil.ReadFile(runtimeFilePath)
	if err != nil {
		return err
	}

	RunTime = &RunTimeConf{}
	err = yaml.Unmarshal([]byte(runtimeFile), &RunTime)
	if err != nil {
		return err
	}
	return nil
}
