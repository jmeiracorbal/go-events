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
	stream, err := a.openStream(ctx)
	if err != nil {
		return StreamInfo{}, err
	}
	info := stream.CachedInfo()
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
	if err := validateSubjectPattern(required); err != nil {
		return false, err
	}
	want := strings.Split(required, ".")
	patterns := make([][]string, 0, len(s.Subjects))
	maxLength := len(want)
	for _, subject := range s.Subjects {
		if err := validateSubjectPattern(subject); err != nil {
			return false, err
		}
		tokens := strings.Split(subject, ".")
		patterns = append(patterns, tokens)
		maxLength = max(maxLength, len(tokens))
	}
	if want[len(want)-1] != ">" {
		return coversLength(patterns, want), nil
	}

	// A trailing > matches one or more tokens. Check every finite length
	// where coverage can change, and one beyond all configured prefixes.
	prefix := want[:len(want)-1]
	for length := len(want); length <= maxLength+1; length++ {
		concrete := make([]string, length)
		copy(concrete, prefix)
		for j := len(prefix); j < length; j++ {
			concrete[j] = "*"
		}
		if !coversLength(patterns, concrete) {
			return false, nil
		}
	}
	return true, nil
}

// At a fixed length a finite union of literal tokens cannot cover an
// arbitrary wildcard token. At least one pattern must cover all tokens.
func coversLength(patterns [][]string, required []string) bool {
	for _, pattern := range patterns {
		matched := true
		for j, token := range pattern {
			if j >= len(required) {
				matched = false
				break
			}
			if token == ">" {
				return true
			}
			if token != "*" && token != required[j] {
				matched = false
				break
			}
		}
		if matched && len(pattern) == len(required) {
			return true
		}
	}
	return false
}
