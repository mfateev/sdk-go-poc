package main

import (
	"errors"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

func main() {
	input, err := workflow.Input()
	if err == nil {
		input, err = workflow.ExecuteActivity("echo", input, time.Minute)
	}
	if err == nil {
		started := time.Now()
		time.Sleep(time.Second)
		if time.Since(started) < time.Second {
			err = errors.New("host clock did not advance for durable timer")
		}
	}
	_ = workflow.Complete(input, err)
}
