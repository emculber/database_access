package postgresql_access

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

func GenerateConfigFile(save_location string) {
	db := DatabaseInfo{}
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Please Enter Host: ")
	db.Host, _ = reader.ReadString('\n')
	db.Host = strings.Replace(db.Host, "\n", "", -1)

	fmt.Print("Please Enter Port: ")
	port, _ := reader.ReadString('\n')
	port = strings.Replace(port, "\n", "", -1)
	db.Port, _ = strconv.Atoi(port)

	fmt.Print("Please Enter Username: ")
	db.Username, _ = reader.ReadString('\n')
	db.Username = strings.Replace(db.Username, "\n", "", -1)

	fmt.Print("Please Enter Password: ")
	db.Password, _ = reader.ReadString('\n')
	db.Password = strings.Replace(db.Password, "\n", "", -1)

	fmt.Print("Please Enter Database Name: ")
	db.Dbname, _ = reader.ReadString('\n')
	db.Dbname = strings.Replace(db.Dbname, "\n", "", -1)

	config := Configuration{db}
	json_config, _ := json.Marshal(config)
	ioutil.WriteFile(save_location+"config.json", json_config, 0644)
}

func ReadDatabase(db *sql.DB, tables []string, columns []string, conditions []string) [][]interface{} {
	select_tables := strings.Join(tables, ", ")
	select_columns := strings.Join(columns, ", ")
	select_conditions := strings.Join(conditions, " AND ")

	statement := "SELECT " + select_columns + " FROM " + select_tables

	if select_conditions != "" {
		statement = statement + " WHERE " + select_conditions
	}
	/*
		fmt.Println(select_tables)
		fmt.Println(select_columns)
		fmt.Println(select_conditions)
		fmt.Println(statement)
	*/

	values, _, err := QueryDatabase(db, statement)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return values
}
