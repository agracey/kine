package pgsql

import (
	"context"
	"strings"
	"net/url"
	"time"

	"database/sql"
	
	"github.com/sirupsen/logrus"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)


var (
	schema = []string{

		// compaction
		// TODO: iterate over all tables and delete what's marked as deleted
		// `CREATE OR REPLACE PROCEDURE compaction () as $$
		  
		// $$ LANGUAGE plpgsql;`,

		// insert
		// TODO: id should probably be incrementing globally, not just per table?
		`CREATE OR REPLACE FUNCTION insert (
			_id INTEGER,
			_name VARCHAR(630),
			_create INTEGER,
			_delete INTEGER,
			_create_revision INTEGER,
			_prev_revision INTEGER,
			_lease INTEGER,
			_value bytea,
			_old_value bytea
		) 
		RETURNS TABLE (
			id INTEGER
		)
		AS $$
			DECLARE 
				_table_name VARCHAR(630) := split_part(_name, '/', 3);

			BEGIN

				IF _table_name = '' THEN
				  _table_name := 'kine';
				  ELSE
					_table_name := 'kine' || _table_name;
				END IF;
				_table_name := '"' || _table_name || '"';

				IF to_regclass(_table_name) IS NULL THEN
				RAISE WARNING '% IS NULL', _table_name;

				  EXECUTE 'CREATE TABLE IF NOT EXISTS ' || _table_name || ' (
					id SERIAL PRIMARY KEY,
					name VARCHAR(630),
					created INTEGER,
					deleted INTEGER,
					create_revision INTEGER,
					prev_revision INTEGER,
					lease INTEGER,
					value bytea,
					old_value bytea
				  );';

				  EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS name ON ' || _table_name || ' (name)';
			    END IF;

				RETURN QUERY EXECUTE format(
					'INSERT  INTO ' || _table_name || 
					' (name, created, deleted, create_revision, prev_revision, lease, value, old_value) 
					VALUES ($2, $3, $4, $5, $6, $7, $8, $9) 
					ON CONFLICT (name) DO UPDATE 
					SET old_value = '|| _table_name ||'.value, id = EXCLUDED.id, prev_revision = '|| _table_name ||'.id
					RETURNING id;')
				USING _table_name, _name, _create, _delete, _create_revision, _prev_revision, _lease, _value, _old_value;

			END
		$$ LANGUAGE plpgsql;`,

		`CREATE OR REPLACE FUNCTION upsert (
			_name VARCHAR(630),
			_create INTEGER,
			_delete INTEGER,
			_lease INTEGER,
			_value bytea
		) 
		RETURNS TABLE (
			id INTEGER
			prev_revision INTEGER
		)
		AS $$
			DECLARE 
				_table_name VARCHAR(630) := split_part(_name, '/', 3);

			BEGIN

				IF _table_name = '' THEN
				  _table_name := 'kine';
				  ELSE
					_table_name := 'kine' || _table_name;
				END IF;
				_table_name := '"' || _table_name || '"';

				IF to_regclass(_table_name) IS NULL THEN
				RAISE WARNING '% IS NULL', _table_name;

				  EXECUTE 'CREATE TABLE IF NOT EXISTS ' || _table_name || ' (
					id SERIAL PRIMARY KEY,
					name VARCHAR(630),
					created INTEGER,
					deleted INTEGER,
					create_revision INTEGER,
					prev_revision INTEGER,
					lease INTEGER,
					value bytea,
					old_value bytea
				  );';

				  EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS name ON ' || _table_name || ' (name)';
			    END IF;

				RETURN QUERY EXECUTE format(
					'INSERT  INTO ' || _table_name || 
					' (name, created, deleted, lease, value) 
					VALUES ($1, $2, $3, $4, $5) 
					ON CONFLICT (name) DO UPDATE 
					SET old_value = '|| _table_name ||'.value, id = EXCLUDED.id, prev_revision = '|| _table_name ||'.id
					RETURNING id, prev_revision ;')
				USING _name, _create, _delete, _lease, _value;
			END
		$$ LANGUAGE plpgsql;`,

		`CREATE OR REPLACE FUNCTION markDeleted(_name VARCHAR(630), _rev INTEGER) 
		  RETURNS TABLE (
			id INTEGER, 
			key INTEGER,
			value bytea,
		  ) AS $$
		DECLARE 
			_table_name VARCHAR(630) := split_part(_name, '/', 3);
		BEGIN
			IF _table_name = '' THEN
				_table_name := 'kine';
			ELSE
				_table_name := 'kine' || _table_name;
			END IF;
			_table_name := '"' || _table_name || '"';

			IF to_regclass(_table_name) IS NULL THEN
				RAISE WARNING '% is null', _table_name;
				RETURN ;
			END IF;

			RETURN QUERY EXECUTE format('UPDATE '
	
		END;
		$$ LANGUAGE plpgsql;`,

		// list
		// TODO clean up after pivot of golang code
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
				_table_name VARCHAR(630) := split_part(_name, '/', 3);
				_startId INTEGER := 0;
	
			BEGIN 
			  IF _table_name = '' THEN
			    _table_name := 'kine';
			  ELSE
			    _table_name := 'kine' || _table_name;
			  END IF;
			  _table_name := '"' || _table_name || '"';

			  IF to_regclass(_table_name) IS NULL THEN
			    RAISE WARNING '% is null', _table_name;
				RETURN ;
			  END IF;
			  
			  IF _startKey != '' THEN
			    _startId := (SELECT MAX(ikv.id) AS id 
			      FROM kine AS ikv 
			      WHERE ikv.name = _startKey
				    AND ikv.id <= _revision);
			  END IF;

			RETURN QUERY EXECUTE format(E'SELECT 
			  id,
			  (
				SELECT 
				  MAX(crkv.prev_revision) AS prev_revision 
				FROM 
				  kine AS crkv 
				WHERE 
				  crkv.name = \'compact_rev_key\'
			  ),
			  id AS theid, 
			  kv.name, 
			  kv.created, 
			  kv.deleted, 
			  kv.create_revision, 
			  kv.prev_revision, 
			  kv.lease, 
			  kv.value, 
			  kv.old_value 
			FROM ' || _table_name || ' AS kv
			WHERE 
			  kv.name LIKE $1
			  AND ( kv.deleted = 0 OR $4 )
			ORDER BY 
			  kv.id ASC
			LIMIT ' || _limit || ';') 
			USING _name, _revision, _startId, _deleted;
		END;
		$$ LANGUAGE plpgsql;`,

		// count
		`CREATE OR REPLACE FUNCTION countkeys(_name VARCHAR(630)) 
		RETURNS TABLE (
			maxid INTEGER,
			count bigint
		)
		AS $$
			DECLARE 
				_table_name VARCHAR(630) := split_part(_name, '/',3);
			BEGIN 

			IF _table_name = '' OR _table_name = 'kine' THEN
			  _table_name := 'kine';
		    ELSE
			  _table_name := 'kine' || _table_name;
		    END IF;

		    _table_name := '"' || _table_name || '"';
			
			IF to_regclass(_table_name) IS NULL THEN
			  RETURN QUERY EXECUTE 'SELECT 0 as maxid, 0::bigint as count;';
			ELSE 
			  RETURN QUERY EXECUTE format('
			    SELECT max(id) as maxid, count(distinct(name)) as count FROM  '|| _table_name );
			END IF;
			
			END
		$$ LANGUAGE plpgsql;`,

		// Keeping around to use for compact_rev_key tracking 
		// TODO: delete after pivot of code
		`CREATE TABLE IF NOT EXISTS kine
 			(
 				id SERIAL PRIMARY KEY,
				name VARCHAR(630),
				created INTEGER,
				deleted INTEGER,
 				create_revision INTEGER,
 				prev_revision INTEGER,
 				lease INTEGER,
 				value bytea,
 				old_value bytea
 			);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_index ON kine (name)`,
		`CREATE INDEX IF NOT EXISTS kine_name_id_index ON kine (name,id)`,
		`CREATE INDEX IF NOT EXISTS kine_id_deleted_index ON kine (id,deleted)`,
		`CREATE INDEX IF NOT EXISTS kine_prev_revision_index ON kine (prev_revision)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
	}
	createDB = "CREATE DATABASE "
)


func setup(db *sql.DB) error {

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

func openAndTest(driverName, dataSourceName string) (*sql.DB, error) {
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