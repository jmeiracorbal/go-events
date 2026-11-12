package events

import (
	"reflect"
	"testing"
)

func TestEventValidation(t *testing.T) {
	for _, event := range []Event{
		{},
		{ID: " ", Topic: "jobs.started"},
		{ID: "1", Topic: ""},
		{ID: "1", Topic: "jobs started"},
	} {
		if err := event.Validate(); err == nil {
			t.Fatalf("expected invalid event: %+v", event)
		}
	}
	if err := (Event{ID: "1", Topic: "jobs.started"}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCloneIsIndependent(t *testing.T) {
	original := Event{ID: "one", Topic: "jobs.started", Data: []byte("hello")}
	copy := original.Clone()
	copy.Data[0] = 'H'
	if !reflect.DeepEqual(original.Data, []byte("hello")) {
		t.Fatalf("clone modified the original payload: %q", original.Data)
	}
}
