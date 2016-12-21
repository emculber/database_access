package postgresql

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/emculber/database_access"
)

var config_struct database.Configuration

func AutoConnect() (*sql.DB, database.Tables, error) {
	path := os.Getenv("GOPATH")
	return ConfigFilePathAutoConnect(path + "/configs/config.json")
}

func ConfigNameAutoConnect(config_name string) (*sql.DB, database.Tables, error) {
	path := os.Getenv("GOPATH")
	return ConfigFilePathAutoConnect(path + "/configs/" + config_name)
}

func ConfigFilePathAutoConnect(config_path string) (*sql.DB, database.Tables, error) {
	var err error

	config_file, err := ioutil.ReadFile(config_path)
	if err != nil {
		return nil, database.Tables{}, err
	}

	//Getting the database sql.DB pointer using the config_file
	db, tables, err := GetDatabaseConnection(config_file)
	if err != nil {
		return nil, database.Tables{}, err
	}

	//Testing the connections to verify we have connected to the database
	_, err = TestDatabaseConnection(db)
	if err != nil {
		return nil, database.Tables{}, err
	}

	return db, tables, nil
}

func AutoCreateTables(db *sql.DB, tables database.Tables) {
	for _, table := range tables.Tables {
		//fmt.Println(table)
		statement := "CREATE TABLE " + table.Name + " ("
		for i, column := range table.Columns {
			if i != 0 {
				statement += ", "
			}
			statement = statement + column.Name + " " + column.Constraint
		}
		statement += ")"
		fmt.Println(statement)
		err := CreateDatabaseTable(db, statement)
		if err != nil {
			panic(err)
		}
	}
}
