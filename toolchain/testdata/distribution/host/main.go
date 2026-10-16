package main

import (
	"example.org/tgo-distribution/jobs"
	"fmt"
	"isolate"
)

func dispatch(h isolate.Handle) {
	if err := h.Invoke(func(...isolate.Value) error { return nil }, func(values ...isolate.Value) error {
		if values[0].Value.(int) != 42 {
			panic("instance state leaked")
		}
		_, err := isolate.Call(1, []byte("ok"))
		return err
	}); err != nil {
		panic(err)
	}
}

func main() {
	h, ok := isolate.LookupFunction(jobs.Entry)
	if !ok {
		panic("missing isolate registration")
	}
	for range 2 {
		i, err := isolate.New(isolate.Config{Program: h.ProgramWithHandle(dispatch)})
		if err != nil {
			panic(err)
		}
		if err = i.Start(); err != nil {
			panic(err)
		}
		select {
		case c := <-i.Commands():
			c.Reply(nil, nil)
		case <-i.Done():
			panic(i.Wait())
		}
		if err = i.Wait(); err != nil {
			panic(err)
		}
	}
	if jobs.HostCount() != 41 {
		panic("host state changed")
	}
	fmt.Println("tgo distribution works")
}
