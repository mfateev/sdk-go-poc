package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestUpstreamGetSessionInfo(t *testing.T) {
	if GetSessionInfo(nil) != nil || GetSessionInfo(context.Background()) != nil {
		t.Fatal("unexpected session")
	}
	parent := &SessionInfo{SessionID: "parent", SessionState: SessionStateFailed}
	ctx := context.WithValue(context.Background(), sessionKey{}, parent)
	if GetSessionInfo(ctx) != parent {
		t.Fatal("session identity changed")
	}
	child := &SessionInfo{SessionID: "child", SessionState: SessionStateClosed}
	derived := context.WithValue(ctx, sessionKey{}, child)
	if GetSessionInfo(derived) != child || GetSessionInfo(ctx) != parent {
		t.Fatal("derived session mutated parent")
	}
}

func TestUpstreamCreationWithOpenSessionContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), sessionKey{}, &SessionInfo{SessionState: SessionStateOpen})
	got, err := CreateSession(ctx, &SessionOptions{CreationTimeout: time.Minute, ExecutionTimeout: time.Minute})
	if got != nil || err == nil || !strings.Contains(err.Error(), "existing open session") {
		t.Fatalf("context=%v error=%v", got, err)
	}
}

func TestUpstreamCompletionWithClosedOrFailedSessionContext(t *testing.T) {
	CompleteSession(context.Background())
	for _, state := range []SessionState{SessionStateClosed, SessionStateFailed} {
		s := &SessionInfo{SessionState: state, cancel: func() { t.Fatal("inactive session was canceled") }}
		CompleteSession(context.WithValue(context.Background(), sessionKey{}, s))
		if s.SessionState != state {
			t.Fatalf("inactive state changed to %v", s.SessionState)
		}
	}
}

func TestUpstreamSessionRecreateToken(t *testing.T) {
	s := &SessionInfo{taskqueue: "resource@host"}
	token := s.GetRecreateToken()
	if string(token) != `{"Taskqueue":"resource@host"}` {
		t.Fatalf("token=%s", token)
	}
	token[0] = '!'
	if string(s.GetRecreateToken()) != `{"Taskqueue":"resource@host"}` {
		t.Fatal("token aliases session state")
	}
}

func TestUpstreamInvalidRecreateToken(t *testing.T) {
	for _, token := range []string{"invalid", "", "null", "{}", `{"Taskqueue":""}`, `{"Taskqueue":1}`} {
		ctx, err := RecreateSession(context.Background(), []byte(token), &SessionOptions{CreationTimeout: time.Minute, ExecutionTimeout: time.Minute})
		if ctx != nil || err == nil {
			t.Fatalf("accepted token %q: %v %v", token, ctx, err)
		}
	}
}

func TestUpstreamSessionCreationValidation(t *testing.T) {
	for _, options := range []*SessionOptions{nil, {}, {CreationTimeout: time.Minute}, {ExecutionTimeout: time.Minute},
		{CreationTimeout: -time.Second, ExecutionTimeout: time.Minute},
		{CreationTimeout: time.Minute, ExecutionTimeout: time.Minute, HeartbeatTimeout: -time.Second}} {
		ctx, err := CreateSession(context.Background(), options)
		if ctx != nil || err == nil {
			t.Fatalf("accepted options %+v", options)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := CreateSession(ctx, &SessionOptions{CreationTimeout: time.Minute, ExecutionTimeout: time.Minute}); got != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled creation: %v %v", got, err)
	}
	if got, err := CreateSession(nil, nil); got != nil || err == nil {
		t.Fatal("accepted nil context")
	}
}
