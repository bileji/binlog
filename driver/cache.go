package driver

import (
	"os"
	"io/ioutil"
)

func Read(path string) (cache string, err error) {
	fi, err := os.Open(path)
	if err != nil {
		return
	}
	defer fi.Close()
	fd, err := ioutil.ReadAll(fi)
	cache = string(fd)
	return
}

func Write(path string, data string) (err error) {
	bytes := []byte(data)
	err = ioutil.WriteFile(path, bytes, 0777)
	return
}
