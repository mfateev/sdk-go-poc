package main

import "testing"

func TestStressGrowthDetectsRetainedResources(t *testing.T) {
	baseline := stressSnapshot{heap: 100, objects: 100, stacks: 100, resident: 100, goroutines: 1, threads: 1, fds: 1}
	for _, tc := range []struct {
		name string
		grow func(*stressSnapshot)
	}{
		{"heap", func(s *stressSnapshot) { s.heap += (16 << 20) + 1 }},
		{"objects", func(s *stressSnapshot) { s.objects += 20001 }},
		{"stacks", func(s *stressSnapshot) { s.stacks += (4 << 20) + 1 }},
		{"rss", func(s *stressSnapshot) { s.resident += (64 << 20) + 1 }},
		{"goroutines", func(s *stressSnapshot) { s.goroutines += 9 }},
		{"threads", func(s *stressSnapshot) { s.threads += 9 }},
		{"fds", func(s *stressSnapshot) { s.fds += 5 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := baseline
			tc.grow(&current)
			if err := stressGrowth(baseline, current); err == nil {
				t.Fatal("retained resource passed leak check")
			}
		})
	}
	if err := stressGrowth(baseline, baseline); err != nil {
		t.Fatal(err)
	}
	if err := stressGrowth(baseline, stressSnapshot{}); err != nil {
		t.Fatalf("resource decrease flagged as a leak: %v", err)
	}
}
