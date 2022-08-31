package pgsql

import (
	"context"
	"net/url"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	//"github.com/sirupsen/logrus"
)

const (
	defaultDSN = "postgres://postgres:postgres@localhost/"
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


// TODO: simplify
func databaseNameFromDSN(dataSourceName string) (string, error) {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}
	if len(u.Path) == 0 || u.Path == "/" {
		u.Path = "/kinetest" //TODO
	}

	name := strings.TrimLeft(u.Path, "/")
	return name, nil
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
		u.Path = "/kinetest"  //TODO
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


