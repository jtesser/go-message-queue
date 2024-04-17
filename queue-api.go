package go_pg_message_queue

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

const getDistinctOrgsSQL = `select sub.* from (SELECT DISTINCT q.owner_organization from queue_%s q) sub order by random()`

const pulseSQL = `UPDATE running_processes SET pulse = now()::timestamp, pulses = pulses + 1 WHERE sub_transaction_id = $1`

const insertMessageSQL = `insert into queue_%s (owner_organization, transaction_id, sub_transaction_id, pulse, create_date_time, process_date_time,
                     topic, retries, pulses, errors, payload, error_messages, content_id, server_id, prev_sub_trans_id)
values ($1, $2, gen_random_uuid(), null, now()::timestamp, null, $3, 0,0,0,$4, null, $5, null, $6);`

const moveProcessToDoneSQL = `WITH message_rows as (
DELETE FROM
      running_processes
  USING (
      SELECT *
      FROM running_processes
      WHERE sub_transaction_id = $1
  ) q
  WHERE q.sub_transaction_id = running_processes.sub_transaction_id RETURNING running_processes.*
)
INSERT INTO done_processes
SELECT owner_organization, transaction_id, sub_transaction_id, pulse, create_date_time, process_date_time, topic, retries, pulses, errors, payload, error_messages, content_id, null, prev_sub_trans_id
FROM message_rows`

const moveErroredProcessBackToQueueSQL = `WITH message_rows as (
DELETE FROM
      running_processes
  USING (
      SELECT *
      FROM running_processes
      WHERE sub_transaction_id = $1
  ) q
  WHERE q.sub_transaction_id = running_processes.sub_transaction_id RETURNING running_processes.*
)
INSERT INTO queue_3
SELECT owner_organization, transaction_id, sub_transaction_id, pulse, now()::timestamp, process_date_time, topic, retries, pulses, errors + 1 as errors1, payload, $2, content_id, null, prev_sub_trans_id
FROM message_rows`

const moveDeadProcessBackToQueueSQL = `WITH message_rows as (
DELETE FROM
      running_processes
  USING (
      SELECT *
      FROM running_processes
      WHERE pulse < (now() - INTERVAL '%s min')
      FOR UPDATE SKIP LOCKED
  ) q
  WHERE q.sub_transaction_id = running_processes.sub_transaction_id RETURNING running_processes.*
)
INSERT INTO queue_3
SELECT owner_organization, transaction_id, sub_transaction_id, pulse, now()::timestamp, process_date_time, topic, retries, pulses, errors, payload, error_messages, content_id, null, prev_sub_trans_id
FROM message_rows`

const getMessagesToProcessSQL = `WITH message_rows as (
  DELETE FROM
      queue_%s deltable
USING (
      SELECT * FROM queue_%s WHERE topic=$1 AND owner_organization = $2 order by create_date_time LIMIT %s
FOR UPDATE SKIP LOCKED
  ) seltable
  WHERE seltable.sub_transaction_id = deltable.sub_transaction_id RETURNING deltable.*
)
INSERT INTO running_processes
SELECT owner_organization, transaction_id, sub_transaction_id, pulse, create_date_time, now()::timestamp,
       topic, retries, pulses, errors, payload, error_messages, content_id, $3, prev_sub_trans_id
FROM message_rows returning *`

const numOfProcessesForServerSQL = `select count(*) as pcount from running_processes where server_id = $1 and topic = $2`

var serverID = ""

var numOfProcessesForTopics = make(map[string]int)

func init() {
	id := getEnv("MESSAGE_QUEUE_SERVERID", "defaultserverid")
	serverID = id + time.Now().String()
}

func getMessagesToProcess(ctx context.Context, topic string) ([]Message, error) {

	orgs0 := getCurrentDistinctOrgs(0)
	orgs1 := getCurrentDistinctOrgs(1)
	orgs2 := getCurrentDistinctOrgs(2)
	orgs3 := getCurrentDistinctOrgs(3)

	messages := make([]Message, 0)

	if len(orgs0) < 1 && len(orgs1) < 1 && len(orgs2) < 1 && len(orgs3) < 1 {
		return messages, nil
	}

	// get env for max number of running processes for topic
	numOfProccessForTopic := numOfProcessesForTopics[topic]
	topicBatchSize := numOfProccessForTopic / 2

	// start db connection and tx
	conn, err := GetConnection(ctx)
	if err != nil {
		log.Errorf("Unable to get a Connection from the Pool: %v\n", err)
		return nil, err
	}
	var tx pgx.Tx
	defer func() {
		if tx != nil {
			err = tx.Commit(ctx)
			if err != nil {
				log.Errorf("Unable to Commit the Transaction: %v\n", err)
				terr := tx.Rollback(ctx)
				if terr != nil {
					log.Errorf("Unable to Rollback the Transaction: %v\n", err)
				}
			}
		}
		conn.Release()
	}()

	// get number of currently running processes for topic to check if more are needed
	currentNumOfProcessForTopic := 0
	row := conn.QueryRow(ctx, numOfProcessesForServerSQL, serverID, topic)
	err = row.Scan(&currentNumOfProcessForTopic)
	if err != nil {
		log.Errorf("Unable to get Count of Running Procs from SQL: %v\n", err)
		return nil, err
	}

	if topicBatchSize <= currentNumOfProcessForTopic {
		// too many currently running processes, don't get more
		return messages, nil
	}

	// only start a transaction if we have orgs and need to get more messages
	tx, err = conn.Begin(ctx)
	if err != nil {
		log.Errorf("Unable to get a Transaction from the Connection: %v\n", err)
		return nil, err
	}

	// iterate through priority levels to get appropriate number of message for each priority
	for priority := 0; priority < 4; priority++ {
		// track number of message required and gotten for this priority
		messagesForPriority := make([]Message, 0)
		orgs := make([]string, 0)
		var numberToSelectForPriority int
		switch priority {
		case 0:
			numberToSelectForPriority = percentOf(topicBatchSize, 0.10)
			orgs = orgs0
		case 1:
			numberToSelectForPriority = percentOf(topicBatchSize, 0.20)
			orgs = orgs1
		case 2:
			numberToSelectForPriority = percentOf(topicBatchSize, 0.30)
			orgs = orgs2
		case 3:
			numberToSelectForPriority = percentOf(topicBatchSize, 0.40)
			orgs = orgs3
		}

		// iterate through distinct organizations for this priority until appropriate percentage of batch size if reached or as many as possible
		for _, org := range orgs {

			// update limit based on number of messages gotten so far
			limit := numberToSelectForPriority - len(messagesForPriority)

			// query to get and move messages from queue to running process table
			messageRows, err := tx.Query(ctx, fmt.Sprintf(getMessagesToProcessSQL, strconv.Itoa(priority), strconv.Itoa(priority), strconv.Itoa(limit)), topic, org, serverID)
			if err != nil {
				log.Errorf("Unable to get execute SQL to get messages: %v\n", err)
				return nil, err
			}

			for messageRows.Next() {
				var message Message
				err = messageRows.Scan(&message.OwnerOrganization,
					&message.TransactionID,
					&message.SubTransactionID,
					nil,
					&message.CreateDateTime,
					nil,
					&message.Topic,
					&message.Retries,
					&message.Pulses,
					&message.Errors,
					&message.Payload,
					nil,
					nil,
					nil,
					nil,
				)
				message.Priority = priority
				if err != nil {
					log.Errorf("Unable Scan Row to get messages from Returned Rows: %v\n", err)
					// don't return error, continue to get more messages
				}
				messagesForPriority = append(messagesForPriority, message)
			}
			// if our number to get for this priority has been reached move to next priority
			if len(messagesForPriority) >= numberToSelectForPriority {
				break
			}
		}
		// if there are returned for this priority, append to overall messages returned
		if len(messagesForPriority) > 0 {
			messages = append(messages, messagesForPriority...)
		}
	}

	return messages, nil
}

func moveDeadProcessesToQueue(ctx context.Context) error {

	conn, err := GetConnection(ctx)
	if err != nil {
		log.Errorf("Unable to get a Connection from the Pool: %v\n", err)
		return err
	}
	defer func() {
		conn.Release()
	}()

	_, err = conn.Exec(ctx, fmt.Sprintf(moveDeadProcessBackToQueueSQL, getEnv("MESSAGE_QUEUE_PING_MINS_TILL_DEAD", "2")))
	if err != nil {
		log.Errorf("Unable to run moveDeadProcessBackToQueueSQL SQL: %v\n", err)
		return err
	}

	return nil
}

func moveErroredProcessToQueue(ctx context.Context, subTransactionID string, errorMessage string) error {

	conn, err := GetConnection(ctx)
	if err != nil {
		log.Errorf("Unable to get a Connection from the Pool: %v\n", err)
		return err
	}
	defer func() {
		conn.Release()
	}()

	_, err = conn.Exec(ctx, moveErroredProcessBackToQueueSQL, subTransactionID, errorMessage)
	if err != nil {
		log.Errorf("Unable to move errored record back to Queue table: %v\n", err)
		return err
	}
	return nil

}

func createMessage(ctx context.Context, message Message, priority int, previousSubTransactionId string) error {

	conn, err := GetConnection(ctx)
	if err != nil {
		log.Errorf("Unable to get a Connection from the Pool: %v\n", err)
		return err
	}
	defer func() {
		conn.Release()
	}()
	if previousSubTransactionId == "" {
		_, err = conn.Exec(ctx, fmt.Sprintf(insertMessageSQL, strconv.Itoa(priority)), message.OwnerOrganization, message.TransactionID, message.Topic, message.Payload, message.ContentId, nil)
	} else {
		_, err = conn.Exec(ctx, fmt.Sprintf(insertMessageSQL, strconv.Itoa(priority)), message.OwnerOrganization, message.TransactionID, message.Topic, message.Payload, message.ContentId, previousSubTransactionId)
	}
	if err != nil {
		log.Errorf("Unable to run Pulse Update SQL: %v\n", err)
		return err
	}
	return nil
}

func markMessageAsDone(ctx context.Context, subTransactionID string) error {

	conn, err := GetConnection(ctx)
	if err != nil {
		log.Errorf("Unable to get a Connection from the Pool: %v\n", err)
		return err
	}
	defer func() {
		conn.Release()
	}()

	_, err = conn.Exec(ctx, moveProcessToDoneSQL, subTransactionID)
	if err != nil {
		log.Errorf("Unable to run Pulse Update SQL: %v\n", err)
		return err
	}
	return nil
}

func markProcessAsCompleted(ctx context.Context, subTransactionID string, nextMessage Message, nextMessagePriority int) error {

	conn, err := GetConnection(ctx)
	if err != nil {
		log.Errorf("Unable to get a Connection from the Pool: %v\n", err)
		return err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Errorf("Unable to get a Transaction from the Connection: %v\n", err)
		return err
	}
	defer func() {
		err = tx.Commit(ctx)
		if err != nil {
			log.Errorf("Unable to Commit the Transaction: %v\n", err)
			terr := tx.Rollback(ctx)
			if terr != nil {
				log.Errorf("Unable to Rollback the Transaction: %v\n", err)
			}
		}
		conn.Release()
	}()

	_, err = tx.Exec(ctx, moveProcessToDoneSQL, subTransactionID)
	if err != nil {
		log.Errorf("Unable to mark process as done in SQL: %v\n", err)
		return err
	}

	_, err = tx.Exec(ctx, fmt.Sprintf(insertMessageSQL, strconv.Itoa(nextMessagePriority)), nextMessage.OwnerOrganization, nextMessage.TransactionID, nextMessage.Topic, nextMessage.Payload, nextMessage.ContentId, subTransactionID)
	if err != nil {
		log.Errorf("Unable to create next message after marking process as done in SQL rolling back entire transaction: %v\n", err)
		return err
	}

	return nil
}

func pulse(ctx context.Context, subTransactionID string) error {

	conn, err := GetConnection(ctx)
	if err != nil {
		log.Errorf("Unable to get a Connection from the Pool: %v\n", err)
		return err
	}
	defer func() {
		conn.Release()
	}()

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Errorf("Unable to get a Transaction from the Connection: %v\n", err)
		return err
	}
	defer func() {
		err = tx.Commit(ctx)
		if err != nil {
			log.Errorf("Unable to Commit the Transaction: %v\n", err)
			terr := tx.Rollback(ctx)
			if terr != nil {
				log.Errorf("Unable to Rollback the Transaction: %v\n", err)
			}
		}
		conn.Release()
	}()

	_, err = tx.Exec(ctx, pulseSQL, subTransactionID)
	if err != nil {
		log.Errorf("Unable to run Pulse Update SQL: %v\n", err)
		return err
	}
	return nil
}

func getDistinctOrgs(ctx context.Context, priority int) ([]string, error) {

	orgs := make([]string, 0)

	conn, err := GetConnection(ctx)
	if err != nil {
		log.Errorf("Unable to get a Connection from the Pool: %v\n", err)
		return orgs, err
	}
	defer func() {
		conn.Release()
	}()

	orgRows, err := conn.Query(ctx, fmt.Sprintf(getDistinctOrgsSQL, strconv.Itoa(priority)))
	for orgRows.Next() {
		org := ""
		err = orgRows.Scan(&org)
		if org != "" {
			orgs = append(orgs, org)
		}
	}
	if err != nil {
		log.Errorf("Unable to get Distinct organizations from SQL: %v\n", err)
		return orgs, err
	}
	return orgs, nil
}
