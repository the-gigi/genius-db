package main

import (
	"fmt"
	"log"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "postgres"
)

func connect() *sql.DB {
	t := "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable"
	connectionString := fmt.Sprintf(t, host, port, user, password, dbname)
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func connectx() *sqlx.DB {
	t := "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable"
	connectionString := fmt.Sprintf(t, host, port, user, password, dbname)
	db := sqlx.MustConnect("postgres", connectionString)
	return db
}


func createSchema(db *sql.DB) {
	schema := `
		CREATE TABLE IF NOT EXISTS genius (
  		id SERIAL PRIMARY KEY,
  		name TEXT UNIQUE,
  		iq INTEGER,
  		nationality TEXT
		);
	`
	_, err := db.Exec(schema)
	if err != nil {
		log.Fatal(err)
	}
}

func createSchemax(db *sqlx.DB) {
	schema := `
		CREATE TABLE IF NOT EXISTS genius (
  		id SERIAL PRIMARY KEY,
  		name TEXT UNIQUE,
  		iq INTEGER,
  		nationality TEXT
		);
	`
	db.MustExec(schema)
}


type Genius struct {
	Name        string `db:"name"`
	IQ          int    `db:"iq"`
	Nationality string `db:"nationality"`
}

func exec(db *sql.DB, command string) {
	_, err := db.Exec(command)
	if err != nil {
		log.Fatal(err)
	}
}

func cleanDB(db *sql.DB) {
	exec(db, "DELETE FROM genius")
}


func populateDB(db *sql.DB) {
	data := []Genius{
		{"Charles Dickens", 165, "English"},
		{"Rafael", 170, "Italian"},
		{"Michael Faraday", 175, "English"},
		{"Baruch Spinoza", 175, "Dutch"},
		{"Michaelangelo", 177, "Italian"},
		{"Desiderius Erasmus", 177, "Dutch"},
		{"Rene Descartes", 177, "French"},
		{"Galileo Galilei", 182, "Italian"},
		{"John Stuart Mill", 182, "English"},
		{"Gottfried Wilhelm Leibnitz", 191, "German"},
		{"Isaac Newton", 192, "English"},
		{"Leonardo Da Vinci", 200, "Italian"},
		{"Johann Wolfgang von Goethe", 220, "German"},
	}

	for _, g := range data {
		t := "INSERT INTO genius (name, iq, nationality) VALUES ('%s', %d, '%s')"
		command := fmt.Sprintf(t, g.Name, g.IQ, g.Nationality)
		exec(db, command)
	}
}


func getEnglishGeniuses(db *sql.DB) {
	rows, err := db.Query("SELECT name, iq FROM genius WHERE nationality='English'")
	if err != nil {
		log.Fatal(err)
	}

	var name string
	var iq int
	for rows.Next() {
		err = rows.Scan(&name, &iq)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("name:", name, "IQ:", iq)
	}
}

func getEnglishGeniusesx(db *sqlx.DB) {
	geniuses := []Genius{}
	db.Select(&geniuses, "SELECT name, iq FROM genius WHERE nationality='English'")

	for _, g := range geniuses {
		fmt.Println("name:", g.Name, "IQ:", g.IQ)
	}
}


func main() {
	//db := connect()
	//defer db.Close()
	//
	//createSchema(db)
	//cleanDB(db)
	//populateDB(db)
	//
	//getEnglishGeniuses(db)

	db := connectx()
	defer db.Close()

	createSchemax(db)
	cleanDB(db.DB)
	populateDB(db.DB)

	getEnglishGeniusesx(db)
}
