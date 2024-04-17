package go_pg_message_queue

import (
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// runWatcherProcess will start a go routine to pulse and handle error returns
func runWatcherProcess(ctx context.Context, topic string, payload []byte, message Message, callback func(message Message, payload []byte) error) error {

	log.WithFields(log.Fields{
		"Topic":            topic,
		"TransactionId":    message.TransactionID,
		"SubTransactionId": message.SubTransactionID,
	}).Info("Starting Watcher Process")

	// Close channel to notify when the process finished
	callbackDone := make(chan Result)

	// Starting a new pulse process and callback
	waitGroup := new(sync.WaitGroup)

	// pulseDone channel is closed by pulseFunc being finished, which is guaranteed by the wait group
	pulseDone := make(chan Result)

	// pulse func
	var pulseFunc = func() {
		// close pulseDone channel when pulse is done
		defer close(pulseDone)
		defer waitGroup.Done()
		for {
			select {
			case <-callbackDone:
				// callbackDone channel was written to or closed
				// do not pulse just exit goroutine
				return
			default:
				// send pulse
				sendPulse(ctx, message.SubTransactionID)
				// sleep x time till next pulse
				time.Sleep(time.Duration(15) * time.Second)
			}
		}
	}
	// add to wait group for pulse routine
	waitGroup.Add(1)
	go pulseFunc()

	var callbackErr error
	var callbackStartTime time.Time
	// add to wait group for callback routine
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		defer close(callbackDone)
		callbackStartTime = time.Now()
		// Executing the given process
		callbackErr = callback(message, payload)
		if callbackErr != nil {

			log.WithFields(log.Fields{
				"Topic":            topic,
				"TransactionId":    message.TransactionID,
				"SubTransactionId": message.SubTransactionID,
			}).Errorf("Error executing callback function: %v", callbackErr)
		}
	}()

	callbackWarningInterval := 120
	callbackWarningTicker := time.NewTicker(time.Duration(callbackWarningInterval) * time.Second)
	defer callbackWarningTicker.Stop()
	// start a func to watch pulse and track callback func ticker
	// adding wait group for ticker/pulse tracker go routine to avoid exit without closing all go routines
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for {
			select {
			// check if callbackDone channel is closed
			case _, ok := <-callbackDone:
				if !ok { // callbackDone channel is closed
					// break for loop
					return
				}
			// check if pulseDone channel is closed
			case _, ok := <-pulseDone:
				if !ok { // pulseDone channel is closed
					// check if callbackDone channel is closed
					if _, ok := <-callbackDone; ok {
						// if callbackDone is still active, restart pulse
						// set new channel
						pulseDone = make(chan Result)
						// add to wait group for pulse routine
						waitGroup.Add(1)
						go pulseFunc()
					}
				}
			case <-callbackWarningTicker.C:
				log.WithFields(log.Fields{
					"TransactionId":     message.TransactionID,
					"SubTransactionId":  message.SubTransactionID,
					"CallbackStartTime": callbackStartTime.String(),
				}).Warnf("callback function has been running longer than %d seconds", callbackWarningInterval)
			}
		}
	}()

	// wait for both go func to finish before returning so we don't have any goroutine leaks
	waitGroup.Wait()

	// if there was an error in callback func, move the process back to queue
	if callbackErr != nil {
		callbackErr = moveErroredProcessToQueue(ctx, message.SubTransactionID, callbackErr.Error())
		if callbackErr != nil {
			log.Errorf("error moving errored process to queue")
			return callbackErr
		}
	}

	// return nil since callback error is handled above
	return nil
}

func sendPulse(ctx context.Context, subTransactionId string) {

	err := pulse(ctx, subTransactionId)
	if err != nil {
		log.Errorf("error sending pulse")
	}
}
