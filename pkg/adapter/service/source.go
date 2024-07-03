package adapter

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/BrobridgeOrg/broton"
	"github.com/spf13/viper"

	gravity_adapter "github.com/BrobridgeOrg/gravity-sdk/v2/adapter"
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	log "github.com/sirupsen/logrus"
)

var counter uint64

type Packet struct {
	EventName string
	Payload   []byte
	lastLSN   string
}

type Source struct {
	adapter   *Adapter
	info      *SourceInfo
	store     *broton.Store
	database  *Database
	connector *gravity_adapter.AdapterConnector
	incoming  chan *CDCEvent
	name      string
	parser    *parallel_chunked_flow.ParallelChunkedFlow
	tables    map[string]SourceTable
	stopping  bool
}

type Request struct {
	Time  int64
	Table string
	Req   *Packet
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &Request{}
	},
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func NewSource(adapter *Adapter, name string, sourceInfo *SourceInfo) *Source {

	// required channel
	if len(sourceInfo.Host) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required host")

		return nil
	}

	if len(sourceInfo.DBName) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required dbname")

		return nil
	}

	if len(sourceInfo.SlotName) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required slotName")

		return nil
	}

	// Prepare table configs
	tables := make(map[string]SourceTable, len(sourceInfo.Tables))
	for tableName, config := range sourceInfo.Tables {
		tables[tableName] = config
	}

	source := &Source{
		adapter:  adapter,
		info:     sourceInfo,
		store:    nil,
		database: NewDatabase(),
		incoming: make(chan *CDCEvent, 204800),
		name:     name,
		tables:   tables,
		stopping: false,
	}

	// Initialize parapllel chunked flow
	pcfOpts := parallel_chunked_flow.Options{
		BufferSize: 204800,
		ChunkSize:  512,
		ChunkCount: 512,
		Handler: func(data interface{}, output func(interface{})) {
			/*
				id := atomic.AddUint64((*uint64)(&counter), 1)
				if id%100 == 0 {
					log.Info(id)
				}
			*/

			req := source.prepareRequest(data.(*CDCEvent))
			if req == nil {
				log.Error("req in nil")
				return
			}

			output(req)
		},
	}

	source.parser = parallel_chunked_flow.NewParallelChunkedFlow(&pcfOpts)

	return source
}

func (source *Source) parseEventName(event *CDCEvent) string {

	eventName := ""

	// determine event name
	tableInfo, ok := source.tables[event.Table]
	if !ok {
		return eventName
	}

	switch event.Operation {
	case InsertOperation:
		eventName = tableInfo.Events.Create
	case UpdateOperation:
		eventName = tableInfo.Events.Update
	case DeleteOperation:
		eventName = tableInfo.Events.Delete
	case SnapshotOperation:
		eventName = tableInfo.Events.Snapshot
	default:
		return eventName
	}

	return eventName
}

func (source *Source) Uninit() error {
	fmt.Println("Stopping ...")
	source.stopping = true
	source.database.stopping = true
	time.Sleep(1 * time.Second)
	<-source.connector.PublishAsyncComplete()
	source.adapter.storeMgr.Close()
	return nil

}

func (source *Source) Init() error {

	if viper.GetBool("store.enabled") {

		// Initializing store
		log.WithFields(log.Fields{
			"store": "adapter-" + source.name,
		}).Info("Initializing store for adapter")
		store, err := source.adapter.storeMgr.GetStore("adapter-" + source.name)
		if err != nil {
			return err
		}

		source.store = store

		// Getting table's lsn
		for tableName, _ := range source.tables {
			// set  default values
			var initialLoadStatus int64 = 0

			// register columns
			columns := []string{"status"}
			err := source.store.RegisterColumns(columns)
			if err != nil {
				log.Error(err)
				return err
			}
			// Getting last Time
			initialLoadStatusCol := fmt.Sprintf("%s", tableName)
			initialLoadStatus, err = source.store.GetInt64("status", []byte(initialLoadStatusCol))
			if err != nil {
				log.Error(err)
				return err
			}

			tableInfo := source.database.tableInfo[tableName]
			if initialLoadStatus != 0 {
				tableInfo.initialLoaded = true
			} else {
				tableInfo.initialLoaded = false
			}
			source.database.tableInfo[tableName] = tableInfo
		}
	}

	// Initializing gravity adapter connector
	source.connector = source.adapter.app.GetAdapterConnector()

	// Connect to database
	err := source.database.Connect(source)
	if err != nil {
		return err
	}

	go source.eventReceiver()
	go source.requestHandler()

	// Getting tables
	tables := make([]string, 0, len(source.tables))
	for tableName, _ := range source.tables {
		tables = append(tables, tableName)
	}

	log.WithFields(log.Fields{
		"tables": tables,
	}).Info("Preparing to watch tables")

	log.Info("Ready to start CDC, tables: ", tables)
	//err = source.database.StartCDC(source.tables, source.info.InitialLoad, source.info.Interval, func(event *CDCEvent) {
	go func(tables map[string]SourceTable, initialLoad bool, initialLoadBatchSize int, interval int) {
		err = source.database.StartCDC(tables, initialLoad, initialLoadBatchSize, interval, func(event *CDCEvent) {

			eventName := source.parseEventName(event)

			// filter
			if eventName == "" {
				//log.Error("eventName is empty")
				return
			}

			source.incoming <- event
		})
		if err != nil {
			log.Fatal(err)
		}
	}(source.tables, source.info.InitialLoad, source.info.InitialLoadBatchSize, source.info.Interval)

	return nil
}

func (source *Source) eventReceiver() {

	log.WithFields(log.Fields{
		"source":      source.name,
		"client_name": source.adapter.clientName + "-" + source.name,
	}).Info("Initializing workers ...")

	for {
		select {
		case msg := <-source.incoming:
			source.parser.Push(msg)
		}
	}
}

func (source *Source) requestHandler() {

	for {
		select {
		case req := <-source.parser.Output():
			// TODO: retry
			if req == nil {
				log.Error("req in nil")
				break
			}
			source.HandleRequest(req.(*Request))
			requestPool.Put(req)
		}
	}
}

func (source *Source) prepareRequest(event *CDCEvent) *Request {

	// determine event name
	eventName := source.parseEventName(event)
	if eventName == "" {
		return nil
	}

	// Prepare payload
	data := make(map[string]interface{}, len(event.Before)+len(event.After))
	for k, v := range event.Before {
		data[k] = v
	}

	for k, v := range event.After {
		data[k] = v
	}

	payload, err := json.Marshal(data)
	if err != nil {
		log.Error(err)
		return nil
	}

	// Preparing request
	request := requestPool.Get().(*Request)
	request.Time = event.Time
	request.Table = event.Table

	request.Req = &Packet{
		EventName: eventName,
		Payload:   payload,
		lastLSN:   event.LastLSN,
	}

	return request
}

func (source *Source) HandleRequest(request *Request) {

	if source.stopping {
		time.Sleep(time.Second)
		return
	}

	for {
		// Using new SDK to re-implement this part
		meta := make(map[string]string)
		meta["Nats-Msg-Id"] = fmt.Sprintf("%s-%s-%s", source.name, request.Table, request.Req.lastLSN)
		_, err := source.connector.PublishAsync(request.Req.EventName, request.Req.Payload, meta)
		if err != nil {
			log.Error("Failed to get publish Request:", err)
			log.Debug("EventName: ", request.Req.EventName, " Payload: ", string(request.Req.Payload))
			time.Sleep(time.Second)
			continue
		}
		log.Debug("EventName: ", request.Req.EventName)
		log.Trace("Payload: ", string(request.Req.Payload))
		break
	}

	id := atomic.AddUint64((*uint64)(&counter), 1)
	if id%10000 == 0 {
		<-source.connector.PublishAsyncComplete()
		counter = 0
	}
	return
}
