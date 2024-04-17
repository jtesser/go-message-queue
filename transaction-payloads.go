package go_pg_message_queue

import "context"

func Store(ctx context.Context, transactionID string, key string, payload string) error {
	return storePayload(ctx, transactionID, key, payload)
}

func Get(ctx context.Context, transactionID string, key string) (payload string, err error) {
	return getPayload(ctx, transactionID, key)
}
