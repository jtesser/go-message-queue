package go_pg_message_queue

import (
	"context"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"time"
)

// Creating a new parser in order to support seconds in the job intervals
var cronJobParser = cron.WithParser(
	cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow))

// If the job is still running we need to skip its execution
var skipIfStillRunning = cron.WithChain(
	cron.SkipIfStillRunning(cron.DefaultLogger),
)

const (
	checkDeadProcessIntervalEnv = "CHECK_DEAD_PROCESS_INTERVAL"
	getActiveProcessIntervalEnv = "GET_ACTIVE_PROCESS_INTERVAL"
	getDistinctOrgsIntervalEnv  = "GET_DISTINCT_ORGS_INTERVAL"
)

func init() {
	startCheckDeadProcess()
}

// startCheckDeadProcess will start the initial cron job to check for dead processes to check for processes that are not pulsing
func startCheckDeadProcess() {
	log.Info("Starting Check Dead Process Cron Job")

	// Check dead process cron job
	checkDeadProcessCron := cron.New(skipIfStillRunning, cronJobParser)
	_, err := checkDeadProcessCron.AddFunc(getEnv(checkDeadProcessIntervalEnv, "*/90 * * * * *"), func() {
		err := moveDeadProcessesToQueue(context.Background())
		if err != nil {
			log.Errorf("error moving dead processes to queue")
		}
	})

	if err != nil {
		log.Errorf("Error creating check dead process cron job - %v", err)
	} else {
		checkDeadProcessCron.Start()
	}
}

// startGetActiveProcess will start the a cron job to get the next set of active processes
func startGetActiveProcess(ctx context.Context, topic string, callback func(message Message, payload []byte) error) error {
	log.Infof("Starting Get Active Process Cron Job for topic [%s]", topic)

	// set max number of processes for this topic
	numOfProcessesForTopics[topic] = getIntEnv(topic, 50)

	go func() {
		err := getActiveProcesses(ctx, topic, callback)
		if err != nil {
			log.Errorf("error getting active processes from queue")
		}

		if err != nil {
			log.Errorf("Error creating get active processes cron job - %v", err)
		}
	}()

	return nil
}

func getActiveProcesses(ctx context.Context, topic string, callback func(message Message, payload []byte) error) error {
	log.Infof("Running Get Active Process Cron Job for topic [%s]", topic)

	sleepInterval := getIntEnv(getActiveProcessIntervalEnv, 100)
	log.Infof("starting infinite loop for active processes")

	for {
		messages, err := getMessagesToProcess(ctx, topic)
		if err != nil {
			log.Errorf("error getting messages to process")
			// we don't return because we don't want this function to end
		}

		for _, message := range messages {
			m := message
			// start watcher process as a go routine.
			go func() {
				err = runWatcherProcess(ctx, m.Topic, []byte(m.Payload), m, callback)
				if err != nil {
					log.Errorf("error running watcher process for topic [%s]", topic)
				}
			}()
		}
		time.Sleep(time.Duration(sleepInterval) * time.Millisecond)
	}
}
