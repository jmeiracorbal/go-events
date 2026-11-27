package nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type RetentionPolicy string

const (
	RetentionLimits RetentionPolicy = "limits"
	RetentionInterest RetentionPolicy = "interest"
	RetentionWorkQueue RetentionPolicy = "work_queue"
)

type StorageType string

const (
	StorageFile StorageType = "file"
	StorageMemory StorageType = "memory"
)

// StreamInfo is a read-only description of the selected JetStream stream.
// It exposes no SDK connection or broker mutation capabilities.
type StreamInfo struct {
	Name string
	Created time.Time
	Subjects []string
	Retention RetentionPolicy
	Storage StorageType
	MaxMessages int64
	MaxMessagesPerSubject int64
	MaxBytes int64
	MaxAge time.Duration
	MaxMessageSize int32
	DuplicateWindow time.Duration
	Replicas int
	Sealed bool
	NoAck bool
	HasSubjectTransform bool
	HasMirror bool
	HasSources bool
	Messages uint64
	FirstSequence uint64
	LastSequence uint64
	Consumers int
}

// InspectStream reads current broker metadata without fetching events,
// creating consumers or changing broker configuration.
func (a *Adapter) InspectStream(ctx context.Context) (StreamInfo, error) {
	stream, err := a.historyStream(ctx)
	if err != nil {
		return StreamInfo{}, err
	}
	info := stream.CachedInfo()
	if info == nil {
		return StreamInfo{}, fmt.Errorf("stream metadata unavailable")
	}
	var retention RetentionPolicy
	switch info.Config.Retention {
	case jetstream.LimitsPolicy:
		retention = RetentionLimits
	case jetstream.InterestPolicy:
		retention = RetentionInterest
	case jetstream.WorkQueuePolicy:
		retention = RetentionWorkQueue
	default:
		return StreamInfo{}, fmt.Errorf("unsupported retention policy %v", info.Config.Retention)
	}
	var storage StorageType
	switch info.Config.Storage {
	case jetstream.FileStorage:
		storage = StorageFile
	case jetstream.MemoryStorage:
		storage = StorageMemory
	default:
		return StreamInfo{}, fmt.Errorf("unsupported storage type %v", info.Config.Storage)
	}
	return StreamInfo{
		Name: info.Config.Name, Created: info.Created,
		Subjects: append([]string(nil), info.Config.Subjects...),
		Retention: retention, Storage: storage,
		MaxMessages: info.Config.MaxMsgs, MaxMessagesPerSubject: info.Config.MaxMsgsPerSubject,
		MaxBytes: info.Config.MaxBytes, MaxAge: info.Config.MaxAge,
		MaxMessageSize: info.Config.MaxMsgSize, DuplicateWindow: info.Config.Duplicates,
		Replicas: info.Config.Replicas, Sealed: info.Config.Sealed, NoAck: info.Config.NoAck,
		HasSubjectTransform: info.Config.SubjectTransform != nil,
		HasMirror: info.Config.Mirror != nil, HasSources: len(info.Config.Sources) != 0,
		Messages: info.State.Msgs, FirstSequence: info.State.FirstSeq,
		LastSequence: info.State.LastSeq, Consumers: info.State.Consumers,
	}, nil
}

// CoversSubject tests whether the configured subject patterns accept every
// concrete subject represented by required. It is a syntax/routing check,
// not proof of authorization, successful publication, or unchanged subjects
// after a server-side transformation.
func (s StreamInfo) CoversSubject(required string) (bool, error) {
	if err := validateHistorySubject(required); err != nil {
		return false, err
	}
	want := strings.Split(required, ".")
	covered := false
	for _, subject := range s.Subjects {
		if err := validateHistorySubject(subject); err != nil {
			return false, err
		}
		if coversPattern(strings.Split(subject, "."), want) {
			covered = true
		}
	}
	return covered, nil
}

func coversPattern(configured, required []string) bool {
	for i, token := range configured {
		if i >= len(required) {
			return false
		}
		if token == ">" {
			return true
		}
		if required[i] == ">" {
			return false
		}
		if token != "*" && token != required[i] {
			return false
		}
	}
	return len(configured) == len(required)
}
