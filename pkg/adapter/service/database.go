package adapter

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

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
}

type tableInfo struct {
	initialLoaded bool
}

func NewDatabase() *Database {
	return &Database{
		dbInfo:      &DatabaseInfo{},
		tableInfo:   make(map[string]tableInfo, 0),
		updateEvent: make(map[int64]CDCEvent, 0),
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

	for tableName, _ := range tables {

		//get tableInfo
		//tableInfo := database.tableInfo[tableName]
		go func() {
			for {
				// query
				sqlStr := fmt.Sprintf(`SELECT * FROM pg_logical_slot_get_changes('%s', NULL, NULL);`,
					database.dbInfo.SlotName,
				)

				//log.Info(sqlStr)
				rows, err := database.db.Queryx(sqlStr)
				if err != nil {
					log.Error(err)
					continue
				}

				for rows.Next() {
					// parse data
					event := make(map[string]interface{}, 0)
					err := rows.MapScan(event)
					if err != nil {
						log.Error(err)
						continue
					}

					var e *CDCEvent
					// Prepare CDC event
					e, err = database.processEvent(tableName, event)
					if err != nil {
						if err == UnsupportEventType {
							log.Warn("Skip event ...")
							continue
						} else if err == CommitEventType || err == BeginEventType {
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

				}

				// delay
				time.Sleep(time.Duration(database.dbInfo.Interval) * time.Second)
				continue

			}
		}()
	}
	return nil

}

func (database *Database) DoInitialLoad(tables map[string]SourceTable, fn func(*CDCEvent)) error {

	for tableName, _ := range tables {
		//get tableInfo
		tableInfo := database.tableInfo[tableName]

		// if scn not equal 0 than don't do it.
		if tableInfo.initialLoaded {
			continue
		}

		tableInfo = database.tableInfo[tableName]

		log.Info("Start initial load.")

		// query
		sqlStr := fmt.Sprintf(`SELECT * FROM %s`,
			//database.dbInfo.DbName,
			tableName,
		)

		log.Info(sqlStr)
		rows, err := database.db.Queryx(sqlStr)
		if err != nil {
			log.Error(err)
		}

		for rows.Next() {
			// parse data
			event := make(map[string]interface{}, 0)
			err := rows.MapScan(event)
			if err != nil {
				log.Error(err)
				continue
			}

			// Prepare CDC event
			e := database.processSnapshotEvent(tableName, event)
			fn(e)
		}

		initialLoadStatusCol := fmt.Sprintf("%s", tableName)
		err = database.source.store.PutInt64("status", []byte(initialLoadStatusCol), 1)
		if err != nil {
			log.Error("Failed to update status")
		}

	}

	return nil

}

func (database *Database) StartCDC(tables map[string]SourceTable, initialLoad bool, interval int, fn func(*CDCEvent)) error {

	// Start query record with batch mode
	for tableName, _ := range tables {
		log.WithFields(log.Fields{
			"Table": tableName,
		}).Info("Received Current Process Time")
	}

	if initialLoad {
		database.DoInitialLoad(tables, fn)
	}

	go database.WatchEvents(tables, interval, fn)

	return nil

}
