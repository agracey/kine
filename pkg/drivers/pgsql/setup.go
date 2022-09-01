package pgsql

import (
	"context"
	"strings"
	"net/url"
	"time"

	"database/sql"
	
	"github.com/sirupsen/logrus"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const (
	defaultMaxIdleConns = 2 // copied from database/sql
)

var (
	schema = []string{

		// compaction
		// TODO: iterate over all tables and delete what's marked as deleted
		// `CREATE OR REPLACE PROCEDURE compaction () as $$
		  
		// $$ LANGUAGE plpgsql;`,

		`CREATE SEQUENCE IF NOT EXISTS revision_seq`,

		// Create -- row := l.queryRow(ctx, "SELECT id FROM upsert($1, 1, 0, $2, $3);", key, lease, value)
		// Update -- row := l.queryRow(ctx, "SELECT id, prev_revision FROM upsert($1, 1, 0, $2, $3);", key, lease, value)

		//upsert
		`CREATE OR REPLACE FUNCTION upsert (
			_name VARCHAR(630),
			_lease INTEGER,
			_value bytea
		) 
		RETURNS TABLE (
			id INTEGER,
			prev_revision INTEGER
		)
		AS $$
			DECLARE 
				_insert_id INTEGER := nextval('revision_seq');
			BEGIN
				RETURN QUERY INSERT INTO kine
				(id, create_revision, name, lease, value) 
				VALUES (_insert_id, _insert_id, _name, _lease, _value) 
					ON CONFLICT (name) DO UPDATE 
					SET id = EXCLUDED.id, prev_revision = kine.id, old_value = kine.value, created = 0, deleted = 0
					RETURNING kine.id, kine.prev_revision;
			END
		$$ LANGUAGE plpgsql;`,

		// Delete - row := l.queryRow(ctx, "SELECT id, key, value FROM markDeleted($1);", key)
		// errRet = row.Scan(&revRet, &deletedKey, &deletedValue)

		//markDeleted
		`CREATE OR REPLACE FUNCTION markDeleted(_name VARCHAR(630)) 
		  RETURNS TABLE (
			id INTEGER, 
			key VARCHAR(630),
			value bytea
		  ) AS $$
		BEGIN
			RETURN QUERY UPDATE kine 
				SET id=nextval('revision_seq'), 
					deleted=1, 
					prev_revision=id, 
					old_value=value, 
					value=DEFAULT 
				WHERE name=_name
				RETURNING id, name, old_value;
		END;
		$$ LANGUAGE plpgsql;`,


		//List -- rows, err := l.query(ctx, "SELECT * FROM list($1, $2);", prefix, limit)
		//Get -- rows, err := l.query(ctx, "SELECT * FROM list($1,$2)", key, limit)
		
		// TODO unsure about crossjoin...  https://www.postgresql.org/docs/current/queries-with.html
		// list
		`CREATE OR REPLACE FUNCTION list(_name VARCHAR(630), _limit INTEGER ) 
		RETURNS TABLE (
			curr_rev BIGINT,
			theid INTEGER,
			name VARCHAR(630),
			created INTEGER,
			deleted INTEGER,
			create_revision INTEGER,
			prev_revision INTEGER,
			lease INTEGER,
			value bytea,
			old_value bytea
		) 
		AS $$
		BEGIN 
			
			IF _limit = 0 THEN 
				RETURN QUERY SELECT 
					rev,
					kv.id, 
					kv.name, 
					kv.created, 
					kv.deleted, 
					coalesce(kv.create_revision,0), 
					coalesce(kv.prev_revision, kv.id), 
					kv.lease, 
					kv.value, 
					kv.old_value 
				FROM kine AS kv
				CROSS JOIN (SELECT last_value as rev FROM revision_seq) as minmax
				WHERE 
				kv.name LIKE _name
				ORDER BY kv.id ASC;
			ELSE
				RETURN QUERY SELECT 
					rev,
					kv.id, 
					kv.name, 
					kv.created, 
					kv.deleted, 
					coalesce(kv.create_revision,0), 
					coalesce(kv.prev_revision, kv.id), 
					kv.lease, 
					kv.value, 
					kv.old_value 
				FROM kine AS kv
				CROSS JOIN (SELECT last_value as rev FROM revision_seq) as minmax
				WHERE 
				kv.name LIKE _name
				ORDER BY 
				kv.id ASC
				LIMIT _limit;
			END IF;
		END;
		$$ LANGUAGE plpgsql;`,

		// Count -- row := l.queryRow(ctx, "SELECT maxid, count FROM countkeys($1);", prefix)
		// count
		`CREATE OR REPLACE FUNCTION countkeys(_name VARCHAR(630)) 
		RETURNS TABLE (
			curr_rev bigint,
			count bigint
		)
		AS $$
			BEGIN 
			RETURN QUERY SELECT (SELECT last_value as curr_rev FROM revision_seq), (SELECT count(distinct(name)) as count FROM kine where name like concat(_name,'%%'));

			END
		$$ LANGUAGE plpgsql;`,

		// Polling -- rows, err := s.query(ctx, "SELECT * from listAll($1);", last)

		//listAll
		`CREATE OR REPLACE FUNCTION listAllSince(_startId INTEGER) 
		RETURNS TABLE (
		  rev BIGINT,
		  theid INTEGER,
		  name VARCHAR(630),
		  created INTEGER,
		  deleted INTEGER,
		  create_revision INTEGER,
		  prev_revision INTEGER,
		  lease INTEGER,
		  value bytea,
		  old_value bytea
		) 
		AS $$
		BEGIN
		RETURN QUERY SELECT 
			curr_rev,
			kv.id, 
			kv.name, 
			kv.created, 
			kv.deleted, 
			coalesce(kv.create_revision,0), 
			coalesce(kv.prev_revision, id), 
			kv.lease, 
			kv.value, 
			kv.old_value 
		FROM kine AS kv
		CROSS JOIN (SELECT last_value as curr_rev FROM revision_seq) as minmax
		WHERE 
			kv.id > _startId
		ORDER BY id ASC;
	  END;
	  $$ LANGUAGE plpgsql;`,


	  `CREATE TABLE IF NOT EXISTS kine (
		id INTEGER PRIMARY KEY DEFAULT NEXTVAL('revision_seq'),
		name VARCHAR(630) UNIQUE,
		created INTEGER DEFAULT 1,
		deleted INTEGER DEFAULT 0,
		create_revision INTEGER,
		prev_revision INTEGER,
		lease INTEGER,
		value bytea,
		old_value bytea
	);`,

	}
	createDB = "CREATE DATABASE "
)


func setup(db *sql.DB, connPoolConfig generic.ConnectionPoolConfig) error {

	if connPoolConfig.MaxIdle < 0 {
		connPoolConfig.MaxIdle = 0
	} else if connPoolConfig.MaxIdle == 0 {
		connPoolConfig.MaxIdle = defaultMaxIdleConns
	}

	logrus.Infof("Configuring postgres database connection pooling: maxIdleConns=%d, maxOpenConns=%d, connMaxLifetime=%s", connPoolConfig.MaxIdle, connPoolConfig.MaxOpen, connPoolConfig.MaxLifetime)
	db.SetMaxIdleConns(connPoolConfig.MaxIdle)
	db.SetMaxOpenConns(connPoolConfig.MaxOpen)
	db.SetConnMaxLifetime(connPoolConfig.MaxLifetime)

	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")
	for _, stmt := range schema {
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil
}


func createDBIfNotExist(dataSourceName string) error {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return err
	}

	dbName := strings.SplitN(u.Path, "/", 2)[1]
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping()
	// check if database already exists
	if _, ok := err.(*pq.Error); !ok {
		return err
	}
	if err := err.(*pq.Error); err.Code != "42P04" {
		if err.Code != "3D000" {
			return err
		}
		// database doesn't exit, will try to create it
		u.Path = "/postgres"
		db, err := sql.Open("postgres", u.String())
		if err != nil {
			return err
		}
		defer db.Close()
		stmt := createDB + dbName + ";"

		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		_, err = db.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

type ConnectionPoolConfig struct {
	MaxIdle     int           // zero means defaultMaxIdleConns; negative means 0
	MaxOpen     int           // <= 0 means unlimited
	MaxLifetime time.Duration // maximum amount of time a connection may be reused
}

func openAndTest(dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}

	for i := 0; i < 3; i++ {
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func configureConnectionPooling(connPoolConfig ConnectionPoolConfig, db *sql.DB) {
	// behavior copied from database/sql - zero means defaultMaxIdleConns; negative means 0
	if connPoolConfig.MaxIdle < 0 {
		connPoolConfig.MaxIdle = 0
	} else if connPoolConfig.MaxIdle == 0 {
		connPoolConfig.MaxIdle = defaultMaxIdleConns
	}

	logrus.Infof("Configuring postgres database connection pooling: maxIdleConns=%d, maxOpenConns=%d, connMaxLifetime=%s", connPoolConfig.MaxIdle, connPoolConfig.MaxOpen, connPoolConfig.MaxLifetime)
	db.SetMaxIdleConns(connPoolConfig.MaxIdle)
	db.SetMaxOpenConns(connPoolConfig.MaxOpen)
	db.SetConnMaxLifetime(connPoolConfig.MaxLifetime)
}

func open(ctx context.Context, dataSourceName string, connPoolConfig ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (*sql.DB, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = openAndTest(dataSourceName)
		if err == nil {
			break
		}

		logrus.Errorf("failed to ping connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	configureConnectionPooling(connPoolConfig, db)

	if metricsRegisterer != nil {
		metricsRegisterer.MustRegister(collectors.NewDBStatsCollector(db, "kine"))
	}

	return db, err
}