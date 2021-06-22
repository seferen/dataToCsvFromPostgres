package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"
)

type dataBase struct {
	Host   string
	Port   int
	User   string
	Pass   string `yaml:"password"`
	DbName string
}

type file struct {
	FileName string `yaml:"fileName"`
	Query    string `yaml:"query"`
}

type Config struct {
	Db    dataBase `yaml:"dataBase"`
	Files []file   `yaml:"files"`
}

func (c *Config) writeQueryToCsv() {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", c.Db.Host, c.Db.Port, c.Db.User, c.Db.Pass, c.Db.DbName)
	log.Println("Connect string:", connStr)
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		return
	}
	defer func() {
		conn.Close(context.Background())
		log.Println("Connect wsa closed succeess")
	}()
	log.Println("Connect to db was open success")

	pg := conn.PgConn()
	defer pg.Close(context.Background())

	for _, file := range c.Files {
		fileCsv, err := os.Create(file.FileName)
		if err != nil {
			log.Println(err)
			return
		}
		defer func(fileName string) {
			fileCsv.Close()
			log.Println("resource file", fileName, "was closed")
		}(file.FileName)
		_, err = pg.CopyTo(context.Background(), fileCsv, "COPY ("+file.Query+") TO STDOUT WITH CSV HEADER")
		if err != nil {
			log.Println(err)
		}

	}

}

func main() {
	data, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		return
	}

	db := Config{}
	err = yaml.Unmarshal(data, &db)
	if err != nil {
		log.Println(err)
		return
	}

	db.writeQueryToCsv()

}
