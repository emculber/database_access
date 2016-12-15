package postgresql_access

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"

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
	return ConfigFilePathAutoConnect(path + "/configs/config.json")
}

func ConfigNameAutoConnect(config_name string) (*sql.DB, error) {
	path := os.Getenv("GOPATH")
	return ConfigFilePathAutoConnect(path + "/configs/" + config_name)
}

func ConfigFilePathAutoConnect(config_path string) (*sql.DB, error) {
	var err error

	config_file, err := os.Open(config_path)
	if err != nil {
		return nil, err
	}

	//Getting the database sql.DB pointer using the config_file
	db, err := GetDatabaseConnection(config_file)
	if err != nil {
		return nil, err
	}

	//Testing the connections to verify we have connected to the database
	_, err = TestDatabaseConnection(db)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func GetDatabaseConnection(config_file *os.File) (*sql.DB, error) {
	var err error
	var config_struct Configuration

	//decoding json config_file and setting it to the config_struct
	decoder := json.NewDecoder(config_file)
	err = decoder.Decode(&config_struct)
	if err != nil {
		return nil, err
	}

	//Setup database connection
	db_url := fmt.Sprintf("postgres://%s:%s@%s/%s", config_struct.Db.Username, config_struct.Db.Password, config_struct.Db.Host, config_struct.Db.Dbname)
	db, err := sql.Open("postgres", db_url)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func ConnectToDatabase(dbname string, host string, port int, username string, password string) *sql.DB {
	db_url := fmt.Sprintf("postgres://%s:%s@%s/%s", username, password, host, dbname)
	db, err := sql.Open("postgres", db_url)
	if err != nil {
		fmt.Println(err)
	}
	return db
}

func TestDatabaseConnection(db *sql.DB) (*sql.Rows, error) {
	//Testing for connectivity
	var err error
	resp, err := db.Query("select version()")
	if err != nil {
		return nil, err
	}
	resp.Close()
	return resp, nil
}

func CreateDatabase(db *sql.DB, create_database_sql string) error {
	var err error

	_, err = db.Query(create_database_sql)
	if err != nil {
		return err
	}
	return nil
}

func CreateDatabaseTable(db *sql.DB, create_table_sql string) error {
	var err error

	//Query to create table with the sql passed
	_, err = db.Query(create_table_sql)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			//Check if table already exists
			//Error code 42P07 is for relation already exists
			if err.Code != "42P07" {
				return err
			}
		}
	}
	return nil
}

func InsertSingleDataValue(db *sql.DB, table_name string, table_columns []string, data []interface{}) error {

	// Transaction Begins and must end with a commit or rollback
	transaction, err := db.Begin()
	if err != nil {
		transaction.Rollback()
		return err
	}

	// Preparing statement with the table name and columns passed
	statement, err := transaction.Prepare(pq.CopyIn(table_name, table_columns...))
	if err != nil {
		statement.Close()
		transaction.Rollback()
		return err
	}

	// Inserting Single Data row into the statement
	_, err = statement.Exec(data...)
	if err != nil {
		statement.Close()
		transaction.Rollback()
		return err
	}

	/*
		_, err = statement.Exec()
		if err != nil {
			return err
		}
	*/

	// Closing the connection of the statement
	err = statement.Close()
	if err != nil {
		statement.Close()
		transaction.Rollback()
		return err
	}

	// Commiting and closing the transaction saving changes we have made in the database
	err = transaction.Commit()
	if err != nil {
		transaction.Rollback()
		return err
	}

	return nil
}

func InsertMultiDataValues(db *sql.DB, table_name string, table_columns []string, data [][]interface{}) error {
	// Transaction Begins and must end with a commit or rollback
	transaction, err := db.Begin()
	if err != nil {
		transaction.Rollback()
		return err
	}

	// Preparing statement with the table name and columns passed
	statement, err := transaction.Prepare(pq.CopyIn(table_name, table_columns...))
	if err != nil {
		statement.Close()
		transaction.Rollback()
		return err
	}

	// Looping though all the data rows passed
	for _, data_row := range data {
		// Inserting Single Data row into the statement
		_, err = statement.Exec(data_row...)
		if err != nil {
			statement.Close()
			transaction.Rollback()
			return err
		}
	}

	/*
		_, err = stmt.Exec()
		if err != nil {
			return err
		}
	*/

	// Closing the connection of the statement
	err = statement.Close()
	if err != nil {
		statement.Close()
		transaction.Rollback()
		return err
	}

	// Commiting and closing the transaction saving changes we have made in the database
	err = transaction.Commit()
	if err != nil {
		transaction.Rollback()
		return err
	}
	return nil
}

func QueryDatabase(db *sql.DB, sql_statment string) ([][]interface{}, int, error) {
	var rowValues [][]interface{}
	var count int = 0

	//Sends the sql statement to the database and retures a set of rows
	rows, err := db.Query(sql_statment)

	defer rows.Close()
	if err != nil {
		return nil, 0, err
	}

	//Gets the Columns for the row set
	cols, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}

	// While there is a next row
	for rows.Next() {

		// making an interface array with the size of columns there are
		vals := make([]interface{}, len(cols))
		val := make([]interface{}, len(cols))

		// Loops though the columns defines the variable types
		for i, _ := range cols {
			//vals[i] = new(sql.RawBytes)
			vals[i] = &val[i]
		}

		// Scanes he row and fills it with the row values for each column
		err = rows.Scan(vals...)
		if err != nil {
			return nil, 0, err
		}

		// Loops though again to convert raw bytes to string vlaues
		for i, val := range vals {
			vals[i] = *(val.(*interface{}))
			var raw_type = reflect.TypeOf(vals[i])

			//fmt.Println(raw_type, vals[i])

			/*
				if raw_bytes, ok := val.(*sql.RawBytes); ok {
					vals[i] = (string(*raw_bytes))
					*raw_bytes = nil // reset pointer to discard current value to avoid a bug
				}
			*/
		}
		// Added string array to list of already converted arrays and adds it to the count
		rowValues = append(rowValues, vals)
		count++
	}
	return rowValues, count, nil
}

func ConvertToStringArray(arr [][]interface{}) string {
	var stringArray string = "ARRAY["
	for x, OuterArr := range arr {
		if x != 0 {
			stringArray += ","
		}
		stringArray += "["
		for i, Value := range OuterArr {
			if i != 0 {
				stringArray += ","
			}
			stringArray += "'" + Value.(string) + "'"
		}
		stringArray += "]"
	}

	stringArray += "]"
	return stringArray
}
