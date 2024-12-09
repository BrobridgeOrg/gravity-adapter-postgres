package adapter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/BrobridgeOrg/broton"
	"github.com/spf13/viper"

	gravity_adapter "github.com/BrobridgeOrg/gravity-sdk/v2/adapter"
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

var counter uint64

type Source struct {
	adapter          *Adapter
	info             *SourceInfo
	store            *broton.Store
	database         *Database
	connector        *gravity_adapter.AdapterConnector
	incoming         chan *CDCEvent
	name             string
	parser           *parallel_chunked_flow.ParallelChunkedFlow
	tables           map[string]SourceTable
	stopping         bool
	ackFutures       []nats.PubAckFuture
	publishBatchSize uint64
}

type Packet struct {
	EventName string
	Payload   []byte
	lastLSN   string
}

type Request struct {
	Time  int64
	Table string
	Req   *Packet
}

var dataPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{})
	},
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &Request{
			Req: &Packet{},
		}
	},
}

var metaPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]string)
	},
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func NewSource(adapter *Adapter, name string, sourceInfo *SourceInfo) *Source {

	viper.SetDefault("gravity.publishBatchSize", 1000)
	publishBatchSize := viper.GetUint64("gravity.publishBatchSize")

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
		adapter:          adapter,
		info:             sourceInfo,
		store:            nil,
		database:         NewDatabase(),
		incoming:         make(chan *CDCEvent, 64),
		name:             name,
		tables:           tables,
		stopping:         false,
		ackFutures:       make([]nats.PubAckFuture, 0, publishBatchSize),
		publishBatchSize: publishBatchSize,
	}

	// Initialize parapllel chunked flow
	pcfOpts := parallel_chunked_flow.Options{
		BufferSize: 2048,
		ChunkSize:  128,
		ChunkCount: 16,
		Handler: func(data interface{}, output func(interface{})) {
			cdcEvent := data.(*CDCEvent)
			defer cdcEventPool.Put(cdcEvent)

			req := source.prepareRequest(cdcEvent)
			if req == nil {
				log.Warn("req in nil")
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
		return ""
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

	source.checkPublishAsyncComplete()
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
			initialLoadStatusCol := fmt.Sprintf("%s-%s", source.name, tableName)
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

	time.Sleep(time.Second)

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
	go func(sourceName string, tables map[string]SourceTable, initialLoad bool, initialLoadBatchSize int, interval int) {
		err = source.database.StartCDC(sourceName, tables, initialLoad, initialLoadBatchSize, interval, func(event *CDCEvent) {

			source.incoming <- event
		})
		if err != nil {
			log.Fatal(err)
		}
	}(source.name, source.tables, source.info.InitialLoad, source.info.InitialLoadBatchSize, source.info.Interval)

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
			for {
				err := source.parser.Push(msg)
				if err != nil {
					log.Trace(err, ", retry ...")
					time.Sleep(10 * time.Millisecond)
					continue
				}
				break
			}
		}
	}
}

func (source *Source) requestHandler() {

	for {
		select {
		case req := <-source.parser.Output():
			// TODO: retry
			/*
				if req == nil {
					log.Error("req in nil")
					break
				}
			*/
			request := req.(*Request)
			source.HandleRequest(request)
			requestPool.Put(request)
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
	data := dataPool.Get().(map[string]interface{})
	defer dataPool.Put(data)
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

	request.Req.EventName = eventName
	request.Req.Payload = payload
	request.Req.lastLSN = event.LastLSN

	return request
}

func (source *Source) HandleRequest(request *Request) {

	if source.stopping {
		time.Sleep(time.Second)
		return
	}

	meta := metaPool.Get().(map[string]string)
	meta["Nats-Msg-Id"] = fmt.Sprintf("%s-%s-%s", source.name, request.Table, request.Req.lastLSN)
	log.Trace("Nats-Msg-Id: ", meta["Nats-Msg-Id"])
	for {
		// Using new SDK to re-implement this part
		future, err := source.connector.PublishAsync(request.Req.EventName, request.Req.Payload, meta)
		if err != nil {
			log.Error("Failed to get publish Request:", err)
			log.Debug("EventName: ", request.Req.EventName, " Payload: ", string(request.Req.Payload))
			time.Sleep(time.Second)
			continue
		}
		source.ackFutures = append(source.ackFutures, future)

		log.Debug("EventName: ", request.Req.EventName)
		log.Trace("Payload: ", string(request.Req.Payload))
		log.Debug("Total amount: ", atomic.AddUint64((*uint64)(&counter), 1))

		metaPool.Put(meta)
		break
	}

	if atomic.LoadUint64((*uint64)(&counter))%source.publishBatchSize == 0 {
		lastFuture := 0
		isError := false
	RETRY:
		for i, future := range source.ackFutures {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			select {
			case <-future.Ok():
				//log.Infof("Message %d acknowledged: %+v", i, pubAck)
			case <-ctx.Done():
				log.Warnf("Failed to publish message, retry ...")
				lastFuture = i
				isError = true
				cancel()
				break RETRY
			}
			cancel()
		}
		if isError {
			source.connector.GetJetStream().CleanupPublisher()
			log.Trace("start retry ...  ", len(source.ackFutures[lastFuture:]))
			for _, future := range source.ackFutures[lastFuture:] {
				// send msg with Sync mode
				for {
					_, err := source.connector.GetJetStream().PublishMsg(future.Msg())
					if err != nil {
						log.Warn(err, ", retry ...")
						time.Sleep(time.Second)
						continue
					}
					break
				}

			}
			log.Trace("retry done")

		}
		source.ackFutures = source.ackFutures[:0]
	}
}

func (source *Source) checkPublishAsyncComplete() {
	// timeout 60s
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	select {
	case <-source.connector.PublishAsyncComplete():
		//log.Info("All messages acknowledged.")
	case <-ctx.Done():
		// if the context timeout or canceled, ctx.Done() will return.
		if ctx.Err() == context.DeadlineExceeded {
			log.Error("Timeout waiting for acknowledgements. AsyncPending: ", source.connector.GetJetStream().PublishAsyncPending())
		}
	}
}
