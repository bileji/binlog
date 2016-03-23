package driver

import (
	"strings"
	"strconv"
	"github.com/bileji/replication"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/client"
)

type BinLog struct {
	Schema   string `json:"schema"`
	Table    string `json:"table"`
	Action   string `json:"action"`
	Values   [][]interface{} `json:"values"`
	File     string
	Position uint32
}

var binLog BinLog

func Start() {
	conf, err := GetConf();
	if err != nil {
		return
	}
	syncEr := replication.NewBinlogSyncer(conf.MysqlConnect.SlaveId, "mysql")
	syncEr.RegisterSlave(conf.MysqlConnect.Host, conf.MysqlConnect.Port, conf.MysqlConnect.Username, conf.MysqlConnect.Password)
	cache, err := Read(conf.CachePath)
	if len(cache) == 0 || err != nil {
		client, err := client.Connect(conf.MysqlConnect.Host + ":" + strconv.Itoa(int(conf.MysqlConnect.Port)), conf.MysqlConnect.Username, conf.MysqlConnect.Password, "")
		if err != nil {
			panic(err)
		}
		status, err := client.Execute("SHOW MASTER STATUS")
		if err != nil {
			panic(err)
		}
		pos, _ := status.GetInt(0, 1)
		file, _ := status.GetString(0, 0)
		streamer, _ := syncEr.StartSync(mysql.Position{file, uint32(pos)})
		binLog.File = file
		for {
			ev, _ := streamer.GetEvent()
			binLog.Position = ev.Header.LogPos
			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				binLog.Action = "write";
				binLog.Values = ev.Event.GetData()
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				binLog.Action = "update";
				binLog.Values = ev.Event.GetData()
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				binLog.Action = "delete";
				binLog.Values = ev.Event.GetData()
			case replication.TABLE_MAP_EVENT:
				tableMap := ev.Event.GetData()
				for _, row := range tableMap {
					for index, element := range row {
						if index == 0 {
							binLog.Schema = element.(string)
						}
						if index == 1 {
							binLog.Table = element.(string)
						}
					}
				}
			case replication.QUERY_EVENT:
				if query := ev.Event.GetData(); len(query) > 0 {
					binLog.Action = "query"
					binLog.Values = query
				}
			}
			if binLog.Position != 0 && len(binLog.Schema) != 0 && len(binLog.Table) != 0 && len(binLog.Action) != 0 && len(binLog.Values) != 0 {
				AddJob(conf.GearManServe, binLog)
				Write(conf.CachePath, binLog.File + "|" + strconv.Itoa(int(binLog.Position)))
				binLog = BinLog{File:binLog.File}
			}
		}
	} else {
		binLogPos := strings.Split(cache, "|")
		pos, err := strconv.Atoi(binLogPos[1])
		if err != nil {
			panic(err)
		}
		file := binLogPos[0]
		streamer, err := syncEr.StartSync(mysql.Position{file, uint32(pos)})
		if err != nil {
			panic(err)
		}
		binLog.File = file
		for {
			ev, _ := streamer.GetEvent()
			binLog.Position = ev.Header.LogPos
			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				binLog.Action = "write";
				binLog.Values = ev.Event.GetData()
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				binLog.Action = "update";
				binLog.Values = ev.Event.GetData()
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				binLog.Action = "delete";
				binLog.Values = ev.Event.GetData()
			case replication.TABLE_MAP_EVENT:
				tableMap := ev.Event.GetData()
				for _, row := range tableMap {
					for index, element := range row {
						if index == 0 {
							binLog.Schema = element.(string)
						}
						if index == 1 {
							binLog.Table = element.(string)
						}
					}
				}
			case replication.QUERY_EVENT:
				if query := ev.Event.GetData(); len(query) > 0 {
					binLog.Action = "query"
					binLog.Values = query
				}
			}
			if ((binLog.Position != 0 && len(binLog.Schema) != 0 && len(binLog.Table) != 0 && len(binLog.Action) != 0) || binLog.Action == "query") && len(binLog.Values) != 0 {
				AddJob(conf.GearManServe, binLog)
				Write(conf.CachePath, binLog.File + "|" + strconv.Itoa(int(binLog.Position)))
				binLog = BinLog{File:binLog.File}
			}
		}
	}
}