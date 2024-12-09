package adapter

import (
	"errors"
	"fmt"
	"sync"

	parser "git.brobridge.com/gravity/gravity-adapter-postgres/pkg/adapter/service/parser"
	log "github.com/sirupsen/logrus"
)

type OperationType int8

const (
	InsertOperation = OperationType(iota + 1)
	UpdateOperation
	DeleteOperation
	SnapshotOperation
)

var (
	UnsupportEventTypeErr = errors.New("Unsupported operation")
	EmptyEventTypeErr     = errors.New("Empty Event Type")
)

type CDCEvent struct {
	Time      int64
	Operation OperationType
	Table     string
	After     map[string]interface{}
	Before    map[string]interface{}
	LastLSN   string
}

var cdcEventPool = sync.Pool{
	New: func() interface{} {
		return &CDCEvent{}
	},
}

func (database *Database) processEvent(event map[string]interface{}) (*CDCEvent, error) {

	// Parse event
	p := parser.NewParser()
	err := p.Parse(event["data"].(string))
	if err != nil {
		log.Error(event["data"].(string))
		return nil, err
	}

	// Prepare CDC event
	e := cdcEventPool.Get().(*CDCEvent)
	e.Table = p.Table
	e.After = p.AfterData

	switch p.Operation {
	case "INSERT":
		e.Operation = InsertOperation
	case "UPDATE":
		e.Operation = UpdateOperation
	case "DELETE":
		e.Operation = DeleteOperation
	case "":
		return nil, EmptyEventTypeErr
	default:
		// Unknown operation
		log.Debug("Skip event:", p.Operation)
		return nil, UnsupportEventTypeErr
	}

	if _, ok := event["lsn"]; ok {
		e.LastLSN = fmt.Sprintf("%s-%s", string(event["lsn"].([]byte)), string(event["xid"].([]byte)))
	} else {
		e.LastLSN = fmt.Sprintf("%s-%s", string(event["location"].([]byte)), string(event["xid"].([]byte)))
	}

	return e, nil
}

func (database *Database) processSnapshotEvent(tableName string, eventPayload map[string]interface{}) *CDCEvent {
	afterValue := make(map[string]interface{})
	for key, value := range eventPayload {
		afterValue[key] = value
	}

	result := cdcEventPool.Get().(*CDCEvent)
	result.Operation = SnapshotOperation
	result.Table = tableName
	result.After = afterValue

	return result

}
