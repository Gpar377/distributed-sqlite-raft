package raft

import (
	"database/sql"
	"fmt"
	"os"
)

type Database struct {
	db       *sql.DB
	filePath string
}

func NewDatabase(filePath string) (*Database, error) {
	// Initialize local SQLite file
	db, err := sql.Open("sqlite3", filePath)
	if (err != nil) {
		return nil, err
	}

	// Verify connectivity
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &Database{
		db:       db,
		filePath: filePath,
	}, nil
}

func (d *Database) Close() error {
	return d.db.Close()
}

// ExecuteWrite applies mutating commands (INSERT, UPDATE, DELETE) inside transaction
func (d *Database) ExecuteWrite(query string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(query)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// ExecuteQuery runs read-only commands
func (d *Database) ExecuteQuery(query string) ([]map[string]interface{}, error) {
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, colName := range cols {
			val := columns[i]
			b, ok := val.([]byte)
			if ok {
				rowMap[colName] = string(b)
			} else {
				rowMap[colName] = val
			}
		}
		results = append(results, rowMap)
	}

	return results, nil
}
