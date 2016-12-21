package database

type Configuration struct {
	Db     Info
	Tables Tables
}

type Info struct {
	Host     string
	Port     int
	Username string
	Password string
	Dbname   string
}

type Tables struct {
	Tables []Table
}

type Table struct {
	Name    string
	Columns []Column
}

type Column struct {
	Name       string
	Constraint string
}
