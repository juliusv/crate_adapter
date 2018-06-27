package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"

	"github.com/go-kit/kit/endpoint"
)

type crateWriteRequest struct {
	Stmt     string
	BulkArgs [][]interface{}
}

type crateReadRequest struct {
	Stmt string
}

type crateReadRow struct {
	labelsHash string
	labels     map[string]string
	timestamp  pgtype.Timestamptz
	value      float64
	valueRaw   int64
}

type crateReadResponse struct {
	Rows []*crateReadRow
}

// TODO: only open connections lazily. (TODO: check if that's already happening)
type dbClient struct {
	conn *pgx.Conn
}

func (c dbClient) endpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		switch r := request.(type) {
		case crateWriteRequest:
			return nil, c.write(r)
		case crateReadRequest:
			return c.read(r)
		default:
			panic("unknown request type")
		}
	}
}

func (c dbClient) write(r crateWriteRequest) error {
	_, err := c.conn.Prepare("write_statement", `INSERT INTO metrics ("labels", "labels_hash", "timestamp", "value", "valueRaw") VALUES ($1, $2, $3, $4, $5)`)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}

	batch := c.conn.BeginBatch()
	for _, a := range r.BulkArgs {
		args := append(make([]interface{}, 0, len(a)), a...)
		batch.Queue(
			"write_statement",
			args,
			[]pgtype.OID{
				pgtype.JSONOID,
				pgtype.VarcharOID,
				pgtype.TimestamptzOID,
				pgtype.Float8OID,
				pgtype.Int8OID,
			},
			nil,
		)
	}

	err = batch.Send(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("error executing write batch: %v", err)
	}

	err = batch.Close()
	if err != nil {
		return fmt.Errorf("error closing write batch: %v", err)
	}
	return nil
}

func (c dbClient) read(r crateReadRequest) (*crateReadResponse, error) {
	rows, err := c.conn.Query(r.Stmt)
	if err != nil {
		return nil, fmt.Errorf("error executing read query: %v", err)
	}
	defer rows.Close()

	resp := &crateReadResponse{}

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
