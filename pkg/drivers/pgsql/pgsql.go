package pgsql

import (
	"context"
	"net/url"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	//"github.com/sirupsen/logrus"
)

const (
	defaultDSN = "postgres://postgres:postgres@localhost/"
	InsertSQL = `SELECT insert(NULL, $1, $2, $3, $4, $5, $6, $7, $8); -- InsertSQL`
	CompactSQL = `CALL compaction($1, $2); -- CompactSQL`
	InsertLastInsertIDSQL = `SELECT insert(NULL, $1, $2, $3, $4, $5, $6, $7, $8); -- InsertLastInsertIDSQL`
	FillSQL = `SELECT insert($1, $2, $3, $4, $5, $6, $7, $8, $9); -- FillSQL`
	CountSQL = `SELECT * FROM countkeys($1::varchar(630)); -- CountSQL`
	AfterSQL = `SELECT * FROM list($1::varchar(630), $3::varchar(630), false, ''::varchar(630), $2::integer); -- AfterSQL`

	// list('compact_rev_key', 'ALL', false, '%%'::varchar(630), 0)

	// ARGS: prefix, revision, includeDeleted, limitString
	ListRevisionStartSQL = `SELECT * FROM list($1::varchar(630), $4::varchar(630), $3, ''::varchar(630), $2::integer); -- ListRevisionStartSQL` 
	
	// ARGS: prefix, revision, startKey, revision, includeDeleted, limitString
	GetRevisionAfterSQL = `SELECT * FROM list($1::varchar(630), $5::varchar(630), $4, $3::varchar(630), $2::integer); -- GetRevisionAfterSQL`

	// ARGS: prefix, includeDeleted, limitString
	GetCurrentSQL = `SELECT * FROM list($1::varchar(630), $3::varchar(630), $2, ''::varchar(630), 0); -- GetCurrentSQL`
)




func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	parsedDSN, err := prepareDSN(dataSourceName, tlsInfo)
	if err != nil {
		return nil, err
	}

	err = createDBIfNotExist(parsedDSN)
	if err != nil {
		return nil, err
	}

	db, err := openAndTest(parsedDSN)
	if err != nil {
		return nil, err
	}
	
	
	err = setup(db, connPoolConfig)
	if err != nil {
		return nil, err
	}

	path, err := databaseNameFromDSN(dataSourceName)
	if err != nil {
		return nil, err
	}

	return &PGStructured{
		DB: db,
		DatabaseName: path,
		notify: make(chan interface{}, 1024),
	}, nil
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


