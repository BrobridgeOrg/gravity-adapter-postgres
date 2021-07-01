package adapter

import (
	"errors"
	"strings"

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
	BeginEventType     = errors.New("Skip BEGIN event")
	CommitEventType    = errors.New("Skip COMMIT event")
)

type CDCEvent struct {
	Time      int64
	Operation OperationType
	Table     string
	After     map[string]*parser.Value
	Before    map[string]*parser.Value
}

func (database *Database) parseInsertSQL(tableName string, event string) (*CDCEvent, error) {

	p := parser.NewParser()
	err := p.Parse(event)
	if err != nil {
		return nil, err
	}

	// Prepare CDC event
	result := CDCEvent{
		Operation: InsertOperation,
		Table:     p.Table,
		After:     p.AfterData,
		Before:    p.BeforeData,
	}

	return &result, nil
}

func (database *Database) parseUpdateSQL(tableName string, event string) (*CDCEvent, error) {

	// Prepare CDC event
	p := parser.NewParser()
	err := p.Parse(event)
	if err != nil {
		return nil, err
	}

	// Prepare CDC event
	result := CDCEvent{
		Operation: UpdateOperation,
		Table:     p.Table,
		After:     p.AfterData,
		Before:    p.BeforeData,
	}

	return &result, nil

}

func (database *Database) parseDeleteSQL(tableName string, event string) (*CDCEvent, error) {

	// Prepare CDC event
	p := parser.NewParser()
	err := p.Parse(event)
	if err != nil {
		return nil, err
	}

	// Prepare CDC event
	result := CDCEvent{
		Operation: DeleteOperation,
		Table:     p.Table,
		After:     p.AfterData,
		Before:    p.BeforeData,
	}

	return &result, nil

}

func (database *Database) processEvent(tableName string, event map[string]interface{}) (*CDCEvent, error) {

	cdcEvent := event["data"].(string)

	insertEvent := strings.Index(cdcEvent, ": INSERT: ")
	if insertEvent >= 0 {
		//Insert Event
		return database.parseInsertSQL(tableName, cdcEvent)
	}

	updateEvent := strings.Index(cdcEvent, ": UPDATE: ")
	if updateEvent >= 0 {
		//Update Before Event
		return database.parseUpdateSQL(tableName, cdcEvent)
	}

	deleteEvent := strings.Index(cdcEvent, ": DELETE: ")
	if deleteEvent >= 0 {
		//Delete Event
		return database.parseDeleteSQL(tableName, cdcEvent)
	}

	beginEvent := strings.Index(cdcEvent, "BEGIN ")
	if beginEvent >= 0 {
		// Skip begin type
		return nil, BeginEventType
	}

	commitEvent := strings.Index(cdcEvent, "COMMIT ")
	if commitEvent >= 0 {
		// Skip begin type
		return nil, CommitEventType
	}

	return nil, UnsupportEventType
}

func (database *Database) processSnapshotEvent(tableName string, eventPayload map[string]interface{}) *CDCEvent {
	afterValue := make(map[string]*parser.Value)
	for key, value := range eventPayload {
		afterValue[key] = &parser.Value{
			Data: value,
		}
	}

	result := CDCEvent{
		Operation: SnapshotOperation,
		Table:     tableName,
		After:     afterValue,
	}
	return &result

}
