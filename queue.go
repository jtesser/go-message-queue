package go_pg_message_queue

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
)

func Subscribe(ctx context.Context, topic string, callback func(message Message, payload []byte) error) error {
	// start cron to get active messages
	err := startGetActiveProcess(ctx, topic, callback)
	if err != nil {
		return err
	}
	return err
}

func MarkMessageAsDone(ctx context.Context, subTransactionID string) error {
	return markMessageAsDone(ctx, subTransactionID)
}

func CreateMessage(ctx context.Context, org string, topic string, payload interface{}, transactionID string, currentSubTransactionID string, priority int, contentId string, doneProcessingMessage bool) error {

	bytePayload, err := json.Marshal(payload)
	if err != nil {
		log.Errorf("error marshalling payload: %v", err)
		return err
	}

	// create message struct
	message := Message{
		OwnerOrganization: org,
		TransactionID:     transactionID,
		Topic:             topic,
		Payload:           string(bytePayload),
		ContentId:         contentId,
	}

	if doneProcessingMessage {
		err = markProcessAsCompleted(ctx, currentSubTransactionID, message, priority)
		if err != nil {
			log.Errorf("error marking process as completed: %v", err)
			return err
		}
	} else {
		err = createMessage(ctx, message, priority, currentSubTransactionID)
		if err != nil {
			log.Errorf("error creating message: %v", err)
			return err
		}
	}

	return nil
}
