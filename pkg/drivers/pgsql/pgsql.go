package pgsql

import (
	"context"
	"database/sql"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"fmt"

	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const (
	defaultDSN = "postgres://postgres:postgres@localhost/"
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
			_created INTEGER,
			_deleted INTEGER,
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
				USING _table_name, _name, _created, _deleted, _create_revision, _prev_revision, _lease, _value, _old_value;

			END
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

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	parsedDSN, err := prepareDSN(dataSourceName, tlsInfo)
	if err != nil {
		return nil, err
	}

	if err := createDBIfNotExist(parsedDSN); err != nil {
		return nil, err
	}

	dialect, err := generic.Open(ctx, "postgres", parsedDSN, connPoolConfig, "$", true, metricsRegisterer)
	if err != nil {
		return nil, err
	}

	path, err := databaseNameFromDSN(dataSourceName)
	if err != nil {
		return nil, err
	}

	dialect.GetSizeSQL = fmt.Sprintf(`SELECT pg_database_size('%s'); -- getsize`, path)

	// ARGS: key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue
	dialect.InsertSQL = `SELECT insert(NULL, $1, $2, $3, $4, $5, $6, $7, $8); -- InsertSQL`

	// ARGS: revision, revision
	dialect.CompactSQL = `CALL compaction($1, $2); -- CompactSQL`

	// ARGS: key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue
	dialect.InsertLastInsertIDSQL = `SELECT insert(NULL, $1, $2, $3, $4, $5, $6, $7, $8); -- InsertLastInsertIDSQL`

	// ARGS: revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0, nil, nil
	dialect.FillSQL = `SELECT insert($1, $2, $3, $4, $5, $6, $7, $8, $9); -- FillSQL`



	// ARGS: prefix, false
	dialect.CountSQL = `SELECT * FROM countkeys($1::varchar(630)); -- CountSQL`


	// full list params:
	//   prefix, limitString, includeDeleted, startKey, revision

	// ARGS: prefix, rev, limitString
	dialect.AfterSQL = `SELECT * FROM list($1::varchar(630), $3::varchar(630), false, ''::varchar(630), $2::integer); -- AfterSQL`

	// list('compact_rev_key', 'ALL', false, '%%'::varchar(630), 0)

	// ARGS: prefix, revision, includeDeleted, limitString
	dialect.ListRevisionStartSQL = `SELECT * FROM list($1::varchar(630), $4::varchar(630), $3, ''::varchar(630), $2::integer); -- ListRevisionStartSQL` 
	
	// ARGS: prefix, revision, startKey, revision, includeDeleted, limitString
	dialect.GetRevisionAfterSQL = `SELECT * FROM list($1::varchar(630), $5::varchar(630), $4, $3::varchar(630), $2::integer); -- GetRevisionAfterSQL`

	// ARGS: prefix, includeDeleted, limitString
	dialect.GetCurrentSQL = `SELECT * FROM list($1::varchar(630), $3::varchar(630), $2, ''::varchar(630), 0); -- GetCurrentSQL`
	
	// unneeded
	dialect.PostCompactSQL = ""

	
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(*pq.Error); ok && err.Code == "23505" {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(*pq.Error); ok {
			return string(err.Code)
		}
		return err.Error()
	}

	if err := setup(dialect.DB); err != nil {
		return nil, err
	}

	//dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect)), nil
}

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

func q(sql string) string {
	regex := regexp.MustCompile(`\?`)
	pref := "$"
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		n++
		return pref + strconv.Itoa(n)
	})
}


func databaseNameFromDSN(dataSourceName string) (string, error) {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}
	if len(u.Path) == 0 || u.Path == "/" {
		u.Path = "/kubernetes"
	}

	return u.Path, nil
}

func prepareDSN(dataSourceName string, tlsInfo tls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		dataSourceName = defaultDSN
	} else {
		dataSourceName = "postgres://" + dataSourceName
	}
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}
	if len(u.Path) == 0 || u.Path == "/" {
		u.Path = "/kubernetes"
	}

	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", err
	}
	// set up tls dsn
	params := url.Values{}
	sslmode := "disable"
	if _, ok := queryMap["sslcert"]; tlsInfo.CertFile != "" && !ok {
		params.Add("sslcert", tlsInfo.CertFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslkey"]; tlsInfo.KeyFile != "" && !ok {
		params.Add("sslkey", tlsInfo.KeyFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslrootcert"]; tlsInfo.CAFile != "" && !ok {
		params.Add("sslrootcert", tlsInfo.CAFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslmode"]; !ok && sslmode != "" {
		params.Add("sslmode", sslmode)
	}
	for k, v := range queryMap {
		params.Add(k, v[0])
	}
	u.RawQuery = params.Encode()
	return u.String(), nil
}
