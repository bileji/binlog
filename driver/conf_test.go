package driver

import (
	"testing"
	"log"
)

func Test_Conf_Run(t *testing.T) {
	conf, _ := GetConf()
	log.Println(conf)
}
