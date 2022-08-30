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

		//get_table_name
		`CREATE OR REPLACE FUNCTION get_table_name (
			_key VARCHAR(630)
		)
		RETURNS VARCHAR(630) 
		AS $$ 
		DECLARE 
			_table_name VARCHAR(630) := split_part(_key, '/', 3);

		BEGIN
			_table_name := '"kine' || _table_name || '"';

			IF to_regclass(_table_name) IS NULL THEN
				EXECUTE 'CREATE TABLE IF NOT EXISTS ' || _table_name || E' (
					id INTEGER PRIMARY KEY DEFAULT NEXTVAL(\'revision_seq\'),
					name VARCHAR(630) UNIQUE,
					created INTEGER,
					deleted INTEGER,
					create_revision INTEGER,
					prev_revision INTEGER,
					lease INTEGER,
					value bytea,
					old_value bytea
				);';
			END IF;

			RETURN _table_name;
		END
		$$ LANGUAGE plpgsql;`,



		// Create -- row := l.queryRow(ctx, "SELECT id FROM upsert($1, 1, 0, $2, $3);", key, lease, value)
		// Update -- row := l.queryRow(ctx, "SELECT id, prev_revision FROM upsert($1, 1, 0, $2, $3);", key, lease, value)

		//upsert
		`CREATE OR REPLACE FUNCTION upsert (
			_name VARCHAR(630),
			_create INTEGER,
			_delete INTEGER,
			_lease INTEGER,
			_value bytea
		) 
		RETURNS TABLE (
			id INTEGER,
			prev_revision INTEGER
		)
		AS $$
			DECLARE 
				_table_name VARCHAR(630) := get_table_name(_name);

			BEGIN
				RETURN QUERY EXECUTE format(
					'INSERT  INTO ' || _table_name || 
					' (id, create_revision, name, created, deleted, lease, value) 
					VALUES ($1, $1, $2, $3, $4, $5, $6) 
					ON CONFLICT (name) DO UPDATE 
					SET old_value = '|| _table_name ||'.value, id = EXCLUDED.id, prev_revision = '|| _table_name ||'.id
					RETURNING id, prev_revision ;')
				USING nextval('revision_seq'), _name, _create, _delete, _lease, _value;

			END
		$$ LANGUAGE plpgsql;`,

		// Delete - row := l.queryRow(ctx, "SELECT id, key, value FROM markDeleted($1, $2);", key, revision)

		//markDeleted
		`CREATE OR REPLACE FUNCTION markDeleted(_name VARCHAR(630), _rev INTEGER) 
		  RETURNS TABLE (
			id INTEGER, 
			key INTEGER,
			value bytea
		  ) AS $$
		DECLARE 
			_table_name VARCHAR(630) := get_table_name(_name);
		BEGIN

			IF to_regclass(_table_name) IS NULL THEN
				RETURN ;
			END IF;

			RETURN QUERY EXECUTE format('UPDATE '|| _table_name ||' 
				SET id=$1, 
					deleted=1, 
					previous_rev=id, 
					old_value=value, 
					value=DEFAULT 
				WHERE name=$2') 				
				USING nextval('revision_seq'), _name ;
	
		END;
		$$ LANGUAGE plpgsql;`,


		//List -- rows, err := l.query(ctx, "SELECT * FROM list($1, $2, $3, $4);", prefix, limit, startKey, revision)
		//Get -- rows, err := l.query(ctx, "SELECT * FROM list($1,$2, $3, $4, $5)", key, limit, includeDeletes, rangeEnd, revision)
		
		// TODO unsure about crossjoin...
		// list
		`CREATE OR REPLACE FUNCTION list(_name VARCHAR(630), _limit VARCHAR(630), _deleted BOOLEAN, _startKey VARCHAR(630), _revision INTEGER ) 
		  RETURNS TABLE (
			id INTEGER, 
			rev_key INTEGER,
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
			DECLARE 
				_table_name VARCHAR(630) := get_table_name(_name);
				_startId INTEGER := 0;
	
			BEGIN 

			  IF to_regclass(_table_name) IS NULL THEN
				RETURN ;
			  END IF;

			  ---IF _startKey != '' 
			  ---THEN
			  ---  EXECUTE format(E'SELECT MAX(ikv.id) AS _startId 
			  ---    FROM %s AS ikv 
			  ---    WHERE ikv.name like \'%s\';' , _table_name, _startKey);
			  ---END IF;
			
			  IF _limit = '0' THEN 
			  	_limit := 'ALL';
			  END IF;

			RETURN QUERY EXECUTE format(E'SELECT 
			  maxid,
			  compact_rev,
			  kv.id AS theid, 
			  kv.name, 
			  kv.created, 
			  kv.deleted, 
			  coalesce(kv.create_revision,0), 
			  coalesce(kv.prev_revision, id), 
			  kv.lease, 
			  kv.value, 
			  kv.old_value 
			FROM ' || _table_name || ' AS kv
			CROSS JOIN (select max(id) as maxid, min(prev_revision) as compact_rev from ' || _table_name || ' ) as minmax
			WHERE 
			  kv.name LIKE $1
			  AND id >= $2 
			  AND id >= $3 
			  AND ( kv.deleted = 0 OR $4 )
			ORDER BY 
			  kv.id ASC
			LIMIT ' || _limit || ';') 
			USING _name, _revision, _startId, _deleted;
		END;
		$$ LANGUAGE plpgsql;`,

		// Count -- row := l.queryRow(ctx, "SELECT maxid, count FROM countkeys($1);", prefix)
		// count
		`CREATE OR REPLACE FUNCTION countkeys(_name VARCHAR(630)) 
		RETURNS TABLE (
			maxid bigint,
			count bigint
		)
		AS $$
			DECLARE 
				_table_name VARCHAR(630) := get_table_name(_name);
			BEGIN 
			
			IF to_regclass(_table_name) IS NULL THEN
			  RETURN QUERY EXECUTE E'SELECT last_value as maxid, 0::bigint as count FROM revision_seq;';
			ELSE 
			  RETURN QUERY EXECUTE format(E'
			    SELECT (SELECT last_value FROM revision_seq), (SELECT count(distinct(name)) as count FROM '|| _table_name ||') ' );
			END IF;
			
			END
		$$ LANGUAGE plpgsql;`,

		// Polling -- rows, err := s.query(ctx, "SELECT * from listAll($1);", last)

		//listAll
		`CREATE OR REPLACE FUNCTION listAll(_startId INTEGER) 
		RETURNS TABLE (
		  maxid INTEGER, 
		  rev_key INTEGER,
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
		  declare
		  	row record;
		  BEGIN 

		  FOR row IN 
		  	SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name LIKE 'kine%'
		  LOOP
			RETURN QUERY EXECUTE format(E'SELECT 
				maxid,
				compact_rev,
				kv.id as theid, 
				kv.name, 
				kv.created, 
				kv.deleted, 
				coalesce(kv.create_revision,0), 
				coalesce(kv.prev_revision, id), 
				kv.lease, 
				kv.value, 
				kv.old_value 
			FROM "' || row.table_name || '" AS kv
			CROSS JOIN (select max(id) as maxid, min(prev_revision) as compact_rev from "' || row.table_name || '" ) as minmax
			WHERE 
				id >= $1
			ORDER BY id ASC') 
			USING _startId;
		  END LOOP;
	  END;
	  $$ LANGUAGE plpgsql;`,

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