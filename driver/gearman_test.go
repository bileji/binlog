package driver

import (
	"log"
	"testing"
	"strings"
	"strconv"
	"github.com/mikespook/gearman-go/worker"
)

func Test_Add_Job_Run(t *testing.T) {
	w := worker.New(worker.OneByOne)
	conf, _ := GetConf()
	w.AddServer("tcp4", conf.GearManServe.Host + ":" + strconv.Itoa(int(conf.GearManServe.Port)))
	w.AddFunc("write", GetData, worker.Unlimited)
	w.AddFunc("update", GetData, worker.Unlimited)
	w.AddFunc("delete", GetData, worker.Unlimited)
	if err := w.Ready(); err != nil {
		log.Fatal(err)
		return
	}
	go w.Work()
}

func GetData(job worker.Job) ([]byte, error) {
	log.Printf("add to gearman data:%s", job.Data())
	data := []byte(strings.ToUpper(string(job.Data())))
	return data, nil
}
