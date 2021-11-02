package adapter

import (
	"errors"

	parser "git.brobridge.com/gravity/gravity-adapter-postgres/pkg/adapter/service/parser"
)

type OperationType int8

const (
	InsertOperation = OperationType(iota + 1)
	UpdateOperation
	DeleteOperation
	SnapshotOperation
)

var (
	UnsupportEventType = errors.New("Unsupported operation")
)

type CDCEvent struct {
	Time      int64
	Operation OperationType
	Table     string
	After     map[string]interface{}
	Before    map[string]interface{}
}

func (database *Database) processEvent(tableName string, event map[string]interface{}) (*CDCEvent, error) {

	// Parse event
	p := parser.NewParser()
	err := p.Parse(event["data"].(string))
	if err != nil {
		return nil, err
	}

	// Prepare CDC event
	e := &CDCEvent{
		Operation: InsertOperation,
		Table:     p.Table,
		After:     p.AfterData,
	}

	switch p.Operation {
	case "INSERT":
		e.Operation = InsertOperation
	case "UPDATE":
		e.Operation = UpdateOperation
	case "DELETE":
		e.Operation = DeleteOperation
	default:
		// Unknown operation
		return nil, UnsupportEventType
	}

	return e, nil
}

func (database *Database) processSnapshotEvent(tableName string, eventPayload map[string]interface{}) *CDCEvent {
	afterValue := make(map[string]interface{})
	for key, value := range eventPayload {
		afterValue[key] = value
	}

	result := CDCEvent{
		Operation: SnapshotOperation,
		Table:     tableName,
		After:     afterValue,
	}
	return &result

}
