package go_pg_message_queue

import (
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

var currentSliceMutex = &sync.RWMutex{}
var currentDistinctOrgs = make(map[int][]string)

func init() {
	// set the priority arrays to avoid nil pointers
	// if new priorities are added, this needs to be updated
	for priority := 0; priority < 4; priority++ {
		currentDistinctOrgs[priority] = make([]string, 0)
	}

	err := manageDistinctOrgs(context.Background())
	if err != nil {
		log.Fatalf("error starting distinct orgs go routine: %v", err)
	}
}

func manageDistinctOrgs(ctx context.Context) error {
	sleepInterval := getIntEnv(getDistinctOrgsIntervalEnv, 500)

	log.Infof("starting infinite loop for distinct orgs")

	go func() {
		for {
			// get distinct orgs for each priority
			// if new priorities are added, this needs to be updated
			for priority := 0; priority < 4; priority++ {
				foundDistinctOrgs, err := getDistinctOrgs(ctx, priority)
				if err != nil {
					log.Errorf("error getting distinct organizations for priority %d", priority)
					// don't return error because we don't want to stop this go routine
				}
				setCurrentDistinctOrgs(priority, foundDistinctOrgs)
			}
			time.Sleep(time.Duration(sleepInterval) * time.Millisecond)
		}
	}()

	return nil
}

func setCurrentDistinctOrgs(priority int, distinctOrgs []string) {
	// get mutex lock before interacting with slices
	currentSliceMutex.Lock()
	defer currentSliceMutex.Unlock()

	// update current distinct orgs
	currentDistinctOrgs[priority] = distinctOrgs
}

func getCurrentDistinctOrgs(priority int) []string {
	// get mutex lock before interacting with slices
	currentSliceMutex.Lock()
	defer currentSliceMutex.Unlock()

	returnedCurrentDistinctOrgs := currentDistinctOrgs[priority]
	return returnedCurrentDistinctOrgs
}
