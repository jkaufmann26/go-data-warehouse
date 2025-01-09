package psql

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func MustOpen(uri string) *sql.DB {
	db, err := Open(uri)
	if err != nil {
		panic(fmt.Errorf("error opening db connection: %w", err))
	}
	return db
}

func Open(uri string) (*sql.DB, error) {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(3)

	return db, nil
}
