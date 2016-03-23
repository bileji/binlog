package driver

import (
	"flag"
	"io/ioutil"
	"encoding/json"
	"os"
	"log"
)

const (
	CONF_ARG_LABEL, DEFAULT_CONF_PATH string = "c", "../conf/binlog.json"
)

// Mysql master 连接
type MysqlConnect struct {
	Host     string `json:"host"`
	Port     uint16 `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	SlaveId  uint32 `json:"slave_id"`
}

// GearMan
type GearManServer struct {
	Host string `json:"host"`
	Port int32 `json:"port"`
}

// Binlog 配置
type BinlogConf struct {
	CachePath     string `json:"cache_path"`
	MysqlConnect  MysqlConnect `json:"mysql"`
	GearManServe  GearManServer `json:"gear_man"`
}

func GetConf() (conf BinlogConf, err error) {
	var confPath string
	flag.StringVar(&confPath, CONF_ARG_LABEL, DEFAULT_CONF_PATH, "config path")
	flag.Parse()
	fileInfo, err := os.Stat(confPath)
	if !(err == nil || os.IsExist(err) || fileInfo != nil) {
		log.Printf("config not exist:%s", confPath)
		return
	}
	bytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		return
	}
	json.Unmarshal(bytes, &conf)
	return
}

