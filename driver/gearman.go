package driver

import (
	"log"
	"strconv"
	"github.com/pquerna/ffjson/ffjson"
	"github.com/mikespook/gearman-go/client"
)

func AddJob(server GearManServer, binlog BinLog) {
	bytes, _ := ffjson.Marshal(binlog)
	log.Printf("data:%s", bytes)
	c, err := client.New("tcp4", server.Host + ":" + strconv.Itoa(int(server.Port)))
	if err != nil {
		panic(err)
	}
	defer c.Close()
	c.ErrorHandler = func(e error) {
		log.Println(e)
	}
	jobHandler := func(resp *client.Response) {
		log.Printf("job handler:%s", resp.Data)
	}
	handler, err := c.Do(binlog.Action, bytes, client.JobNormal, jobHandler)
	if err != nil {
		panic(err)
	}
	log.Printf("handler:%s", handler)
}


