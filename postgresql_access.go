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
	Db DatabaseInfo
}

func AutoConnect() (*sql.DB, error) {
	path := os.Getenv("GOPATH")
	configFile, err := os.Open(path + "/src/stockDatabase/config.json")
	if err != nil {
		log.WithFields(log.Fields{
			"Config File": configFile,
			"File":        "postgresql_access.go",
			"Error":       err.Error(),
		}).Error("Error Opening File")
		return nil, err
	}

	db, err := GetDatabaseConnection(configFile)
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Could Not Connect To Database")
		return nil, err
	}

	err = TestDatabaseConnection(db)
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Database Test Connection Failed")
		return nil, err
	}

	return db, nil
}

func GetDatabaseConnection(configFile *os.File) (*sql.DB, error) {

	var err error
	var config Configuration
	decoder := json.NewDecoder(configFile)
	err = decoder.Decode(&config)
	if err != nil {
		log.WithFields(log.Fields{
			"File":              "postgresql_access.go",
			"Method":            "GetDatabaseConnection",
			"Json Encoded File": configFile.Name(),
			"Error":             err.Error(),
		}).Error("Could Not decode json file")
		return nil, err
	}

	log.WithFields(log.Fields{
		"Username":      config.Db.Username,
		"Password":      config.Db.Password,
		"Host":          config.Db.Host,
		"Database Name": config.Db.Dbname,
	}).Debug("Database config variables")

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
	resp, err := db.Query("select version()")
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"SQL Command":      "select version()",
		"Databse Response": resp,
	}).Debug("Database Connection Test Was Successful")
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
				log.WithFields(log.Fields{
					"File":        "postgresql_access.go",
					"SQL Command": createTableSQL,
					"ERROR":       err.Error(),
				}).Error("Error Creating Table")
				return err
			} else {
				log.WithFields(log.Fields{
					"SQL Command": createTableSQL,
				}).Warn("Table already created")
			}
		} else {
			log.WithFields(log.Fields{
				"SQL Command": createTableSQL,
			}).Debug("Table Was Successfully Created")
		}
	}
	return nil
}

//Single Value insert
func InsertSingleDataValue(db *sql.DB, tableName string, tableColumns []string, data []interface{}) error {
	txn, err := db.Begin()
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Error on database Begin")
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(tableName, tableColumns...))
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Error on preparing data")
		return err
	}
	log.WithFields(log.Fields{
		"Table Name":   tableName,
		"Table Colums": tableColumns,
		"Column Size":  len(tableColumns),
		"Data":         data,
	}).Debug("Preparing table")

	_, err = stmt.Exec(data...)
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Error on Preparing Exec data")
		return err
	}
	log.WithFields(log.Fields{
		"data row": data,
		//"data row size": len(d),
	}).Debug("Preparing data")

	_, err = stmt.Exec()
	if err != nil {
		log.WithFields(log.Fields{
			"File":     "postgresql_access.go",
			"data row": data,
			"Error":    err.Error(),
		}).Error("Error on Exec data")
		return err
	}

	err = stmt.Close()
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Error on Closing")
		return err
	}

	err = txn.Commit()
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Error on Committing Data")
		return err
	}

	return nil
}

func InsertMultiDataValues(db *sql.DB, tableName string, tableColumns []string, data [][]interface{}) error {
	txn, err := db.Begin()
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Error on database Begin")
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(tableName, tableColumns...))
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Error on preparing data")
		return err
	}
	log.WithFields(log.Fields{
		"Table Name":   tableName,
		"Table Colums": tableColumns,
		"Column Size":  len(tableColumns),
		"Data":         data,
	}).Debug("Preparing table")

	for _, DataRow := range data {
		log.WithFields(log.Fields{
			"data row": DataRow,
			//"data row size": len(d),
		}).Info("Preparing data")
		_, err = stmt.Exec(DataRow...)
		if err != nil {
			log.WithFields(log.Fields{
				"File":  "postgresql_access.go",
				"Error": err.Error(),
			}).Error("Error on Preparing Exec data")
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.WithFields(log.Fields{
			"File":     "postgresql_access.go",
			"data row": data,
			"Error":    err.Error(),
		}).Error("Error on Exec data")
		return err
	}

	err = stmt.Close()
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Error on Closing")
		return err
	}

	err = txn.Commit()
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Error on Committing Data")
		return err
	}

	return nil
}

func QueryDatabase(db *sql.DB, stmt string) ([][]interface{}, int, error) {
	rows, err := db.Query(stmt)
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"stmt":  stmt,
			"Error": err.Error(),
		}).Error("Error on Query Data")
		return nil, 0, err
	}
	cols, err := rows.Columns() // Remember to check err afterwards
	if err != nil {
		log.WithFields(log.Fields{
			"File":  "postgresql_access.go",
			"Error": err.Error(),
		}).Error("Error Getting Columns")
		return nil, 0, err
	}
	var rowValues [][]interface{}
	var count int = 0
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		for i, _ := range cols {
			vals[i] = new(sql.RawBytes)
		}
		err = rows.Scan(vals...)
		if err != nil {
			log.WithFields(log.Fields{
				"File":  "postgresql_access.go",
				"Error": err.Error(),
			}).Error("Error When Scanning")
			return nil, 0, err
		}
		for i, val := range vals {
			//s := reflect.ValueOf(val)
			if rb, ok := val.(*sql.RawBytes); ok {
				vals[i] = (string(*rb))
				*rb = nil // reset pointer to discard current value to avoid a bug
			}
		}
		rowValues = append(rowValues, vals)
		count++
		log.WithFields(log.Fields{
			"rowValue": vals,
		}).Debug("Row Finished")
	}
	return rowValues, count, nil
}
