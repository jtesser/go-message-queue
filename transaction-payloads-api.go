package go_pg_message_queue

import (
	"context"
	log "github.com/sirupsen/logrus"
)

const insertPayloadSQL = `insert into transaction_payloads values
($1::uuid,$2, $3)
ON CONFLICT (transaction_id, key)
DO
    UPDATE SET
               transaction_id=transaction_payloads.transaction_id,
               key=transaction_payloads.key,
               payload=EXCLUDED.payload
;`

const selectPayloadSQL = `select payload from transaction_payloads where transaction_id=$1::uuid and key=$2 LIMIT 1`

func storePayload(ctx context.Context, transactionID string, key string, payload string) error {

	conn, err := GetConnection(ctx)
	if err != nil {
		log.Errorf("Unable to get a Connection from the Pool: %v\n", err)
		return err
	}
	defer func() {
		conn.Release()
	}()

	_, err = conn.Exec(ctx, insertPayloadSQL, transactionID, key, payload)
	if err != nil {
		log.Errorf("Unable to run insertPayloadSQL SQL: %v\n", err)
		return err
	}
	return nil
}

func getPayload(ctx context.Context, transactionID string, key string) (payload string, err error) {

	conn, err := GetConnection(ctx)
	if err != nil {
		log.Errorf("Unable to get a Connection from the Pool: %v\n", err)
		return "", err
	}
	defer func() {
		conn.Release()
	}()

	row := conn.QueryRow(ctx, selectPayloadSQL, transactionID, key)
	err = row.Scan(&payload)
	if err != nil {
		//log.Errorf("Unable to run selectPayloadSQL SQL: %v\n", err)
		return "", err
	}
	return payload, nil
}
