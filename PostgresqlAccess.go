package main

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

type databaseInfo struct {
	Host     string
	Port     int
	Username string
	Password string
	Dbname   string
}

/*
func accessDatabase() (*sql.DB, error) {
	//Setup database connection
	dbUrl := fmt.Sprintf("postgres://%s:%s@%s/%s", "Erik", "", "localhost", "stockinformationdb")
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	//Testing for connectivity
	_, err = db.Query("select version()")
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	_, err = db.Query(CREATE_SQL)
	if err != nil {
		log.Println(err.Error())
		if err, ok := err.(*pq.Error); ok {
			//Check if table already exists
			//Error code 42P07 is for relation already exists
			if err.Code != "42P07" {
				log.Println(err.Error())
				return nil, err
			} else {
				log.Println("Table already created")
			}
		}
	}

	return db, nil
}
*/

func main() {

	path := os.Getenv("GOPATH")
	fmt.Println(path)

	var FilePath string = path + "/logs/default/default.log"
	fmt.Println(FilePath)
	log.SetOutput(&lumberjack.Logger{
		Filename:   FilePath,
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28, //days
	})
}
