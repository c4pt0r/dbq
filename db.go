package dbq

import (
	"database/sql"
)

var (
	_db *sql.DB
)

func InitDB(db *sql.DB) {
	_db = db
}

func DB() *sql.DB {
	return _db
}
