package adapter

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

var eventPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{})
	},
}

type DatabaseInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	DbName   string `json:"db_name"`
	Param    string `json:"param"`
	SlotName string `json:"slotName"`
	Interval int    `json:"interval"`
}

type Database struct {
	db          *sqlx.DB
	dbInfo      *DatabaseInfo
	tableInfo   map[string]tableInfo
	updateEvent map[int64]CDCEvent
	source      *Source
	stopping    bool
}

type tableInfo struct {
	initialLoaded bool
}

func NewDatabase() *Database {
	return &Database{
		dbInfo:      &DatabaseInfo{},
		tableInfo:   make(map[string]tableInfo, 0),
		updateEvent: make(map[int64]CDCEvent, 0),
		stopping:    false,
	}
}

func (database *Database) Connect(source *Source) error {

	info := source.info
	log.WithFields(log.Fields{
		"host":     info.Host,
		"port":     info.Port,
		"username": info.Username,
		"dbname":   info.DBName,
		"param":    info.Param,
	}).Info("Connecting to database")

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?%s",
		info.Username,
		info.Password,
		info.Host,
		info.Port,
		info.DBName,
		info.Param,
	)

	// Open database
	db, err := sqlx.Open("postgres", connStr)
	if err != nil {
		log.Error(err)
		return err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	database.dbInfo = &DatabaseInfo{
		Host:     info.Host,
		Port:     info.Port,
		Username: info.Username,
		DbName:   info.DBName,
		Param:    info.Param,
		SlotName: info.SlotName,
		Interval: info.Interval,
	}

	database.db = db
	database.source = source

	return nil
}

func (database *Database) GetConnection() *sqlx.DB {
	return database.db
}

func (database *Database) WatchEvents(tables map[string]SourceTable, interval int, fn func(*CDCEvent)) error {

	log.Info("Start watch event.")

	go func() {
		for {
			// query
			sqlStr := fmt.Sprintf(`SELECT * FROM pg_logical_slot_get_changes('%s', NULL, NULL);`,
				database.dbInfo.SlotName,
			)

			//log.Info(sqlStr)
			rows, err := database.db.Queryx(sqlStr)
			if err != nil {
				log.Error("slot: ", err)
				time.Sleep(time.Duration(database.dbInfo.Interval) * time.Second)
				continue
			}

			for rows.Next() {
				// parse data
				event := eventPool.Get().(map[string]interface{})
				err := rows.MapScan(event)
				if err != nil {
					log.Error(err)
					continue
				}

				var e *CDCEvent
				// Prepare CDC event
				e, err = database.processEvent(event)
				if err != nil {
					if err == UnsupportEventTypeErr {
						log.Debug("Skip event ...")
						continue
					} else if err == EmptyEventTypeErr {
						continue
					} else {
						log.Error(err)
						// delay
						timer := time.NewTimer(1 * time.Second)
						<-timer.C
						continue
					}
				}

				fn(e)
				eventPool.Put(event)

			}
			rows.Close()

			// delay
			time.Sleep(time.Duration(database.dbInfo.Interval) * time.Second)
		}
	}()

	return nil

}

func (database *Database) DoInitialLoad(sourceName string, tables map[string]SourceTable, fn func(*CDCEvent), initialLoadBatchSize int, interval int) error {

	if initialLoadBatchSize == 0 {
		initialLoadBatchSize = 100000
	}

	regenSlot := false
	for tableName, _ := range tables {
		//get tableInfo
		tableInfo := database.tableInfo[tableName]

		// if scn not equal 0 than don't do it.
		if tableInfo.initialLoaded {
			continue
		}
		regenSlot = true

		tableInfo = database.tableInfo[tableName]

		//get total amount
		sqlStr := fmt.Sprintf(`SELECT COUNT(*) FROM %s`,
			tableName,
		)

		log.Debug(sqlStr)
		var t interface{}
		var total int64
		err := database.db.Get(&t, sqlStr)
		if err != nil {
			log.Error(err)
		}

		if float64Val, ok := t.(float64); ok {
			total = int64(float64Val)

		}
		if int64Val, ok := t.(int64); ok {
			total = int64Val
		}
		if strVal, ok := t.(string); ok {
			if t, err := strconv.ParseInt(strVal, 10, 64); err == nil {
				total = t
			}
		}

		bulkSize := int64(initialLoadBatchSize)
		remainder := total % bulkSize
		amountByBulk := (total - remainder) / bulkSize

		// query
		// begin transation
		tx, err := database.db.Beginx()
		if err != nil {
			log.Error(err)
		}
		defer tx.Rollback()

		// generate cursor
		_, err = tx.Exec(fmt.Sprintf("DECLARE pagination_cursor CURSOR FOR SELECT * FROM %s ORDER BY ctid", tableName))
		if err != nil {
			log.Error("cursor: ", err)
		}

		for l := int64(1); l <= amountByBulk+1; l++ {
			if l <= amountByBulk {
				from := (l - 1) * bulkSize
				log.Info(fmt.Sprintf("Processing %s initialLoad from %d to %d total: %d", tableName, from, from+bulkSize, total))
			} else if remainder != 0 {
				from := (l - 1) * bulkSize
				log.Info(fmt.Sprintf("Processing %s initialLoad from %d to %d total: %d", tableName, from, from+remainder, total))
			} else {
				continue
			}
			rows, err := tx.Queryx(fmt.Sprintf("FETCH FORWARD %d FROM pagination_cursor", bulkSize))
			if err != nil {
				log.Error("Fetch :", err)
			}

			i := 0
			for rows.Next() {
				// parse data
				event := eventPool.Get().(map[string]interface{})
				err := rows.MapScan(event)
				if err != nil {
					log.Error("mapScan: ", err)
					continue
				}

				// Prepare CDC event
				e := database.processSnapshotEvent(tableName, event)
				i += 1
				e.LastLSN = fmt.Sprintf("%s-%s-%d-%d", sourceName, tableName, l, i)
				fn(e)
				eventPool.Put(event)
			}

			if err := rows.Err(); err != nil {
				if database.stopping {
					return nil
				}
				log.Error("Initialization Error: ", err)
				time.Sleep(time.Duration(interval) * time.Second)
			}

			rows.Close()
		}

		// close cursor
		_, err = tx.Exec("CLOSE pagination_cursor")
		if err != nil {
			log.Error("close cursor: ", err)
		}

		// commit transation
		err = tx.Commit()
		if err != nil {
			log.Error("commit: ", err)
		}

		initialLoadStatusCol := fmt.Sprintf("%s-%s", sourceName, tableName)
		err = database.source.store.PutInt64("status", []byte(initialLoadStatusCol), 1)
		if err != nil {
			log.Error("Failed to update status")
		}
		log.Info(tableName, " initialLoad done.")

	}

	if regenSlot {
		// Re-gen slot
		err := database.regenerateSlot()
		if err != nil {
			log.Error(err)
		}
		log.Info("Re-generate slot: ", database.dbInfo.SlotName)
	}

	return nil

}

func (database *Database) regenerateSlot() error {
	// drop
	log.Debug("Drop Slot")
	sqlStr := fmt.Sprintf(`SELECT pg_drop_replication_slot('%s')`,
		database.dbInfo.SlotName,
	)
	database.db.Exec(sqlStr)

	// create
	log.Debug("Create Slot")
	sqlStr = fmt.Sprintf(`SELECT * FROM pg_create_logical_replication_slot('%s', 'test_decoding')`,
		database.dbInfo.SlotName,
	)
	_, err := database.db.Exec(sqlStr)
	if err != nil {
		return err
	}
	return nil
}

func (database *Database) StartCDC(sourceName string, tables map[string]SourceTable, initialLoad bool, initialLoadBatchSize int, interval int, fn func(*CDCEvent)) error {

	// Start query record with batch mode
	for tableName, _ := range tables {
		log.WithFields(log.Fields{
			"Table": tableName,
		}).Info("Received Current Process Time")
	}

	if initialLoad {
		database.DoInitialLoad(sourceName, tables, fn, initialLoadBatchSize, interval)
	}

	go database.WatchEvents(tables, interval, fn)

	return nil

}
