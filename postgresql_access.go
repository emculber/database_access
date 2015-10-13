package postgresql_access

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/lib/pq"
)

type DatabaseInfo struct {
	Host     string
	Port     int
	Username string
	Password string
	Dbname   string
}

type Configuration struct {
	Db       DatabaseInfo
	Filename string
}

func GetDatabaseConnection(configFile *os.File) (*sql.DB, error) {

	var err error
	var config Configuration
	decoder := json.NewDecoder(configFile)
	err = decoder.Decode(&config)
	if err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{
		"Username":      config.Db.Username,
		"Password":      config.Db.Password,
		"Host":          config.Db.Host,
		"Database Name": config.Db.Dbname,
	}).Info("Database config variables")

	//Setup database connection
	dbUrl := fmt.Sprintf("postgres://%s:%s@%s/%s", config.Db.Username, config.Db.Password, config.Db.Host, config.Db.Dbname)
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func TestDatabaseConnection(db *sql.DB) error {
	//Testing for connectivity
	var err error
	_, err = db.Query("select version()")
	if err != nil {
		return err
	}
	return nil
}

func CreateDatabaseTable(db *sql.DB, createTableSQL string) error {

	var err error
	_, err = db.Query(createTableSQL)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			//Check if table already exists
			//Error code 42P07 is for relation already exists
			if err.Code != "42P07" {
				return err
			} else {
				log.WithFields(log.Fields{
					"SQL Command": createTableSQL,
				}).Warn("Table already created")
			}
		}
	}
	return nil
}

func InsertData(db *sql.DB, tableName string, tableColumns []string, data [][]string) error {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(tableName, tableColumns...))
	log.WithFields(log.Fields{
		"Table Name":   tableName,
		"Table Colums": tableColumns,
	}).Debug("Preparing table")
	if err != nil {
		return err
	}

	for _, d := range data {
		_, err = stmt.Exec(d)
		log.WithFields(log.Fields{
			"data row": d,
		}).Debug("Preparing data")
		if err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}
