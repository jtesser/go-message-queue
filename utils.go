package go_pg_message_queue

import (
	log "github.com/sirupsen/logrus"
	"math"
	"os"
	"strconv"
)

// GetEnv gets a string value from an environment variable
// or return the given default value if the environment variable is not set
//
// Params:
//
//	envVariable : environment variable
//	defaultValue : value to return if environment variable is not set
//
// Returns the string value for the specified variable
func getEnv(envVariable string, defaultValue string) string {

	log.Debugf("Setting value for: %s", envVariable)

	returnValue := defaultValue
	log.Debugf("Default value for %s : %s", envVariable, defaultValue)

	if envStr := os.Getenv(envVariable); len(envStr) > 0 {
		returnValue = envStr
		log.Debugf("Init value for %s set to: %s", envVariable, envStr)
	}

	return returnValue
}

// GetIntEnv get an int value from an environment variable or return the given default value if the environment
// variable is not set.
//
// Params:
//
//	envVariable : environment variable
//	defaultValue : value to return if environment variable is not set
//
// Returns the int value for the specified variable
func getIntEnv(envVariable string, defaultValue int) int {

	log.Debugf("Setting value for: %s", envVariable)

	returnValue := defaultValue
	log.Debugf("Default value for %s : %d", envVariable, defaultValue)

	if envStr := os.Getenv(envVariable); len(envStr) > 0 {

		envValue, err := strconv.Atoi(envStr)
		if err != nil {
			log.Errorf("Couldn't get %s from env variable: %v", envVariable, err)
			return defaultValue
		}

		returnValue = envValue
		log.Debugf("Init value for %s set to: %d", envVariable, envValue)
	}

	return returnValue
}

func percentOf(num int, percent float64) int {
	return int(math.Round(float64(num) * percent))
}
