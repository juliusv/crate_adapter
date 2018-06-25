package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/lib/pq"
)

type crateWriteRequest struct {
	Stmt     string
	BulkArgs [][]interface{}
}

type crateReadRequest struct {
	Stmt string
}

type crateReadRow struct {
	timestamp  time.Time
	labelsHash string
	labels     []byte
	value      float64
	valueRaw   int64
}

type crateReadResponse struct {
	Cols []string
	Rows []*crateReadRow
}

// TODO: only open connections lazily. (TODO: check if that's already happening)
type dbClient struct {
	db *sql.DB
}

func (c dbClient) endpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		switch r := request.(type) {
		case crateWriteRequest:
			fmt.Println("WRITE REQUEST")
			return nil, c.write(r)
		case crateReadRequest:
			fmt.Println("READ REQUEST")
			return c.read(r)
		default:
			panic("unknown request type")
		}
	}
}

func (c dbClient) write(r crateWriteRequest) error {
	// for _, a := range r.BulkArgs {
	// 	_, err := c.db.Exec(r.Stmt, a...)
	// 	if err != nil {
	// 		return fmt.Errorf("error executing write statement: %v", err)
	// 	}
	// }
	// return nil

	// See https://godoc.org/github.com/lib/pq#hdr-Bulk_imports for efficient bulk imports.
	txn, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("error openening write transaction: %v", err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("metrics", "labels", "labels_hash", "value", "valueRaw", "timestamp"))
	if err != nil {
		return fmt.Errorf("error preparing write statement: %v", err)
	}

	for _, a := range r.BulkArgs {
		_, err = stmt.Exec(a...)
		if err != nil {
			return fmt.Errorf("error executing write query: %v", err)
		}
	}

	// Flush buffered data.
	_, err = stmt.Exec()
	if err != nil {
		return fmt.Errorf("error flushing write query: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		return fmt.Errorf("error closing write query: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		return fmt.Errorf("error committing write query: %v", err)
	}
	return nil
}

func (c dbClient) read(r crateReadRequest) (*crateReadResponse, error) {
	rows, err := c.db.Query(r.Stmt)
	if err != nil {
		return nil, fmt.Errorf("error executing read query: %v", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting read request columns: %v", err)
	}

	resp := &crateReadResponse{
		Cols: cols,
	}

	for rows.Next() {
		rr := &crateReadRow{}
		if err := rows.Scan(&rr.labels, &rr.labelsHash, &rr.timestamp, &rr.value, &rr.valueRaw); err != nil {
			return nil, fmt.Errorf("error scanning read request rows: %v", err)
		}
		resp.Rows = append(resp.Rows, rr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through read request rows: %v", err)
	}
	return resp, nil
}
