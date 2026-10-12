// Package upstream ports Temporal's concurrency tests to native Go workflows.
package upstream

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

func check(ok bool, message string) {
	if !ok {
		panic(message)
	}
}

//go:isolate
func NativeWorkflow(ctx context.Context, scenario string) (string, error) {
	switch scenario {
	case "preferred-version":
		first := workflow.GetVersion(ctx, "preferred-version", workflow.DefaultVersion, 1)
		second := workflow.GetVersion(ctx, "preferred-version", workflow.DefaultVersion, 1)
		check(first == workflow.DefaultVersion && second == first, "preferred version was not cached")
	case "returned-panic":
		fc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter())})
		return "", fc.FailureToError(&failurepb.Failure{Message: "panicError", StackTrace: "stackTrace", Source: "GoSDK", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "PanicError"}}})

	case "unbuffered":
		channel := make(chan string)
		done := make(chan struct{})
		go func() { channel <- "one"; channel <- "two"; close(done) }()
		check(<-channel == "one", "first unbuffered value")
		check(<-channel == "two", "second unbuffered value")
		<-done
	case "buffered":
		channel := make(chan string, 1)
		channel <- "one"
		check(len(channel) == 1, "buffered send did not buffer")
		received := make(chan string, 2)
		go func() { received <- <-channel; received <- <-channel }()
		channel <- "two"
		check(<-received == "one" && <-received == "two", "buffered channel order")
	case "blocked-receive":
		channel := make(chan *int)
		started := make(chan struct{})
		done := make(chan struct{})
		go func() { close(started); check(*(<-channel) == 42, "pointer receive"); close(done) }()
		<-started
		value := 42
		channel <- &value
		<-done
	case "closed":
		channel := make(chan int, 1)
		channel <- 7
		close(channel)
		value, more := <-channel
		check(value == 7 && more, "close discarded buffer")
		value, more = <-channel
		check(value == 0 && !more, "closed receive did not return zero")
		select {
		case value, more = <-channel:
			check(value == 0 && !more, "closed select")
		default:
			panic("closed channel not ready")
		}
	case "send-closed", "async-send-closed":
		channel := make(chan int)
		close(channel)
		recovered := false
		func() {
			defer func() { recovered = recover() != nil }()
			if scenario == "send-closed" {
				channel <- 7
			} else {
				select {
				case channel <- 7:
				default:
				}
			}
		}()
		check(recovered, "send on closed channel did not panic")
	case "blocked-send-closed":
		channel := make(chan int)
		started := make(chan struct{})
		recovered := make(chan bool, 1)
		go func() { defer func() { recovered <- recover() != nil }(); close(started); channel <- 7 }()
		<-started
		runtime.Gosched()
		close(channel)
		check(<-recovered, "blocked send did not panic on close")
	case "select-default":
		first, second := make(chan string, 1), make(chan string, 1)
		first <- "one"
		select {
		case value := <-first:
			check(value == "one", "selected wrong first value")
		case <-second:
			panic("empty second channel selected")
		default:
			panic("default selected over ready receive")
		}
		second <- "two"
		select {
		case <-first:
			panic("empty first channel selected")
		case value := <-second:
			check(value == "two", "selected wrong second value")
		default:
			panic("default selected over ready receive")
		}
		usedDefault := false
		select {
		case <-first:
		case <-second:
		default:
			usedDefault = true
		}
		check(usedDefault, "default not selected")
	case "select-blocking":
		first, second := make(chan int), make(chan int)
		done := make(chan struct{}, 2)
		go func() { first <- 1; done <- struct{}{} }()
		go func() { second <- 2; done <- struct{}{} }()
		total := 0
		for range 2 {
			select {
			case value := <-first:
				total += value
				first = nil
			case value := <-second:
				total += value
				second = nil
			}
		}
		check(total == 3, "blocking select lost value")
		<-done
		<-done
	case "select-send":
		channel := make(chan int)
		done := make(chan int, 1)
		go func() {
			select {
			case value := <-channel:
				done <- value
			}
		}()
		select {
		case channel <- 42:
		}
		check(<-done == 42, "select send lost value")
	case "select-async-send":
		channel := make(chan int, 1)
		select {
		case channel <- 42:
		default:
			panic("buffered select send blocked")
		}
		select {
		case channel <- 43:
			panic("full buffer accepted send")
		default:
		}
		check(<-channel == 42, "async send lost value")
	case "select-default-blocking":
		first, second := make(chan string, 1), make(chan string)
		go func() { first <- "one"; second <- "two" }()
		select {
		case <-make(chan struct{}):
			panic("unready case selected")
		default:
			check(<-second == "two", "blocking default result")
		}
		check(<-first == "one", "blocking default lost other value")
	case "cancel", "cancel-children":
		parent, cancel := context.WithCancel(ctx)
		children := make([]context.Context, 64)
		for i := range children {
			var stop context.CancelFunc
			children[i], stop = context.WithCancel(parent)
			defer stop()
		}
		cancel()
		cancel()
		check(errors.Is(parent.Err(), context.Canceled), "parent cancellation identity")
		for _, child := range children {
			<-child.Done()
			check(errors.Is(child.Err(), context.Canceled), "child cancellation lost")
		}
	case "cancel-detach":
		parent, cancel := context.WithCancel(context.WithValue(ctx, "key", "value"))
		child, cancelChild := context.WithCancel(parent)
		detached := context.WithoutCancel(parent)
		cancelChild()
		check(parent.Err() == nil && child.Err() == context.Canceled, "child canceled parent")
		cancel()
		check(detached.Done() == nil && detached.Err() == nil && detached.Value("key") == "value", "disconnected context")
	case "cancel-race":
		parent, cancel := context.WithCancel(ctx)
		done := make(chan struct{}, 64)
		for range 64 {
			go func() { child, stop := context.WithCancel(parent); stop(); <-child.Done(); done <- struct{}{} }()
		}
		go cancel()
		for range 64 {
			<-done
		}
		<-parent.Done()
	case "waitgroup":
		var group sync.WaitGroup
		values := make([]int, 32)
		for i := range values {
			group.Go(func() { values[i] = i + 1 })
		}
		group.Wait()
		group.Wait()
		for i, value := range values {
			check(value == i+1, "wait group returned before work completed")
		}
	case "waitgroup-multiple":
		var group sync.WaitGroup
		gate, done := make(chan struct{}), make(chan struct{}, 2)
		group.Add(1)
		go func() { <-gate; group.Done() }()
		go func() { group.Wait(); done <- struct{}{} }()
		go func() { group.Wait(); done <- struct{}{} }()
		close(gate)
		<-done
		<-done
	case "waitgroup-negative":
		var group sync.WaitGroup
		recovered := false
		func() { defer func() { recovered = recover() != nil }(); group.Add(-1) }()
		check(recovered, "negative wait group counter did not panic")
	case "timer", "buffered-timeout", "unbuffered-timeout":
		started := time.Now()
		channel := make(chan int)
		if scenario == "buffered-timeout" {
			channel = make(chan int, 1)
		}
		select {
		case <-channel:
			panic("empty channel received")
		case fired := <-time.After(time.Second):
			check(fired.Sub(started) == time.Second, "timer timestamp")
		}
		check(time.Since(started) == time.Second, "virtual clock did not advance")
	case "timer-condition":
		channel := make(chan int, 1)
		go func() { <-time.After(time.Second); channel <- 42 }()
		timeout := time.NewTimer(time.Minute)
		defer timeout.Stop()
		select {
		case value := <-channel:
			check(value == 42, "condition result")
		case <-timeout.C:
			panic("condition timed out")
		}
	case "timer-context-cancel":
		waitCtx, cancel := context.WithCancel(ctx)
		timer := time.NewTimer(time.Hour)
		cancel()
		select {
		case <-waitCtx.Done():
			check(errors.Is(waitCtx.Err(), context.Canceled), "timer wait cancellation identity")
		case <-timer.C:
			panic("canceled wait reached timer")
		}
		check(timer.Stop(), "canceled wait failed to stop timer")
	case "timer-cancel":
		timer := time.NewTimer(time.Hour)
		check(timer.Stop(), "active timer could not be stopped")
		select {
		case <-timer.C:
			panic("stopped timer fired")
		default:
		}
	case "future", "future-select", "future-split-join":
		ctx = workflow.WithStartToCloseTimeout(ctx, time.Minute)
		first := workflow.ExecuteActivity(ctx, "echo", 1)
		second := workflow.ExecuteActivity(ctx, "echo", 2)
		if scenario == "future" {
			for range 2 {
				var a, b int
				check(first.Get(ctx, &a) == nil && second.Get(ctx, &b) == nil && a == 1 && b == 2, "future repeatable result")
			}
		} else {
			a, b := first.ToChannel(), second.ToChannel()
			sum := 0
			for range 2 {
				select {
				case result := <-a:
					var value int
					check(result.Err == nil && result.Value.Get(&value) == nil, "first future extraction")
					sum += value
					a = nil
				case result := <-b:
					var value int
					check(result.Err == nil && result.Value.Get(&value) == nil, "second future extraction")
					sum += value
					b = nil
				}
			}
			check(sum == 3, "split join lost future result")
		}
	case "future-failure":
		ctx = workflow.WithStartToCloseTimeout(ctx, time.Minute)
		f := workflow.ExecuteActivity(ctx, "fail", 1)
		check(f.Get(ctx, nil) != nil, "future failure lost")
		result := <-f.ToChannel()
		check(result.Err != nil && result.Value == nil, "future failure channel lost error")
	case "defer":
		done := make(chan int, 1)
		go func() { defer func() { done <- 42 }() }()
		check(<-done == 42, "goroutine defer did not run")
	case "recursive-children":
		done := make(chan struct{}, 1)
		var start func(int)
		start = func(n int) {
			if n == 0 {
				done <- struct{}{}
				return
			}
			go start(n - 1)
		}
		go start(32)
		<-done
	default:
		panic("unknown upstream scenario: " + scenario)
	}
	return "ok", nil
}
