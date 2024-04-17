package go_pg_message_queue

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"os"
)

var connPool *pgxpool.Pool

// Package level initialization.
//
// init functions are automatically executed when the programs start
func init() {
	ctx := context.Background()
	startConnectionPool(ctx, os.Getenv("POSTGRES_CONNECTION_STRING"))
}

// StartConnection starts a new connection pool
func startConnectionPool(ctx context.Context, dbconnString string) error {

	log.Info("Creating Postgres Connection Pool")

	var err error
	connPool, err = pgxpool.New(ctx, dbconnString)

	if err != nil {
		log.Errorf("Unable to create connection pool: %v\n", err)
		return err
	}

	log.Infof("Postgres Connection Pool Created")
	return nil
}

// Get a connection from the pool
func GetConnection(ctx context.Context) (*pgxpool.Conn, error) {
	return connPool.Acquire(ctx)
}
