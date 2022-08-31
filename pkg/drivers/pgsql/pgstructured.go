package pgsql

import (
	"context"
	"strconv"
	"fmt"
	"time"
	"database/sql"
	"strings"
	"github.com/lib/pq"

	//"github.com/Rican7/retry/backoff"
	//"github.com/Rican7/retry/strategy"
	"github.com/sirupsen/logrus"
	
	"github.com/k3s-io/kine/pkg/util"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/broadcaster"
)


type PGStructured struct {
	DB *sql.DB
	DatabaseName string
	broadcaster broadcaster.Broadcaster
	notify      chan interface{} //TODO rename
	pollCtx context.Context
}

func (s *PGStructured) Start(ctx context.Context) error {
	//TODO Start "compaction" (really just deleting what's marked as deleted)


	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if _, err := s.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			logrus.Errorf("Failed to create health check key: %v", err)
		}
	}

	s.startPoll(ctx)

	//TODO go l.ttl(ctx)
	return nil
}

func (l *PGStructured) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	defer func() {
		logrus.Tracef("GET %s, rev=%d => rev=%d, kv=%v, err=%v", key, revision, revRet, kvRet != nil, errRet)
	}()

	rev, event, err := l.get(ctx, key, rangeEnd, limit, revision, false)
	if event == nil {
		return rev, nil, err
	}
	return rev, event.KV, err
}


func (l *PGStructured) get(ctx context.Context, key, rangeEnd string, limit, revision int64, includeDeletes bool) (int64, *server.Event, error) {
	logrus.Tracef("GET %s limit=%d, revision=%d, rangeEnd=%s", key, limit, revision, rangeEnd)
	
	rows, err := l.query(ctx, "SELECT * FROM list($1,$2, $3, $4, $5)", key, limit, includeDeletes, rangeEnd, revision)
	rev, _, events, err := RowsToEvents(rows)
	if err == server.ErrCompacted {
		// ignore compacted when getting by revision
		err = nil
	}
	if err != nil {
		return 0, nil, err
	}
	if revision != 0 {
		rev = revision
	}
	if len(events) == 0 {
		return rev, nil, nil
	}
	logrus.Tracef("GOT %s rev=%d, #events %d, %v", key, rev, len(events), events[0])
	return rev, events[0], nil
}


func (l *PGStructured) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	defer func() {
		logrus.Tracef("CREATE %s, size=%d, lease=%d => rev=%d, err=%v", key, len(value), lease, revRet, errRet)
	}()

	row := l.queryRow(ctx, "SELECT id FROM upsert($1, 1, 0, $2, $3);", key, lease, value)
	errRet = row.Scan(&revRet)
	if errRet != nil {
		return 0, errRet
	}
	
	return
}

func (l *PGStructured) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	defer func() {
		logrus.Tracef("DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v", key, revision, kvRet != nil, deletedRet, errRet)
	}()

	var (
		deletedKey string
		deletedValue []byte
	)

	row := l.queryRow(ctx, "SELECT id, key, value FROM markDeleted($1, $2);", key, revision)
	errRet = row.Scan(&revRet, &deletedKey, &deletedValue)
	if errRet != nil {
		return 0, nil, false, errRet
	}

	//TODO: will delete events get created in the watch stream? Need to check
	// select {
	// 	case l.notify <- revRet:
	// 	default:
	// }
	
	kvRet = &server.KeyValue {
		Key: deletedKey,
		CreateRevision: revRet,
		ModRevision: revRet,
		Value: deletedValue,
		Lease: 0,
	}

	return revRet, kvRet, true, nil
}


func (l *PGStructured) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	// defer func() {
	// 	logrus.Tracef("LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v", prefix, startKey, limit, revision, revRet, len(kvRet), errRet)
	// }()

	logrus.Tracef("LIST %s limit=%s, startKey=%s, revision=%d ", prefix, limit, startKey, revision)
	limitStr := "ALL"
	if limit != 0 {
		limitStr = strconv.FormatInt(limit, 10)
	}

	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	prefix += "%"

	logrus.Tracef("LISTING %s limitStr=%s, startKey=%s, revision=%d ", prefix, limitStr, startKey, revision)
	rows, err := l.query(ctx, "SELECT * FROM list($1, $2, false, $3, $4);", prefix, limitStr, startKey, revision)
	if err != nil {
		return 0,nil,err
	}
	revRet, _, events, err:= RowsToEvents(rows)
	if err != nil {
		logrus.Errorf("fail to convert rows when listing: %v", err)
		return 0,nil,err
	}
	logrus.Tracef("LISTED %s rev=%d, #events %d", prefix, revRet, len(events))

	//Fix revision number if no rows returned
	if revRet == 0 {
		row := l.queryRow(ctx, "SELECT last_value from revision_seq;")
		err := row.Scan(&revRet)

		if err != nil {
			logrus.Errorf("fail to get latest revision: %v", err)
			return 0,nil,err
		}
	}

	kvRet = make([]*server.KeyValue, 0, len(events))
	for _, event := range events {
		kvRet = append(kvRet, event.KV)
	}
	return revRet, kvRet, nil
}


func (l *PGStructured) Count(ctx context.Context, prefix string) (revRet int64, count int64, err error) {

	defer func() {
		logrus.Tracef("COUNT %s => rev=%d, count=%d, err=%v", prefix, revRet, count, err)
	}()

	if strings.HasSuffix(prefix, "/") {
		prefix += "%"
	}

	row := l.queryRow(ctx, "SELECT maxid, count FROM countkeys($1);", prefix)
	err = row.Scan(&revRet, &count)
	return revRet, count, err
}

func (l *PGStructured) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	var (
		prev_revision int64
	)
	defer func() {
		logrus.Tracef("UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, updated=%v, err=%v", key, len(value), revision, lease, revRet, updateRet, errRet)
	}()

	row := l.queryRow(ctx, "SELECT id, prev_revision FROM upsert($1, 1, 0, $2, $3);", key, lease, value)
	errRet = row.Scan(&revRet, &prev_revision)
	if errRet != nil {
		return 0, nil, false, errRet
	}

	updateEvent := &server.Event{
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: revRet, // TODO event.KV.CreateRevision,
			Value:          value,
			Lease:          lease,
		},
		// TODO add this back in -- PrevKV: event.KV,
	}

	updateEvent.KV.ModRevision = revRet
	return revRet, updateEvent.KV, true, nil
}

// Polling changes since we want to see and return changes from other instances as well 
func (s *PGStructured) startPoll(ctx context.Context) (error) {

	var (
		rev     int64
	)
	
	row := s.queryRow(ctx, "SELECT last_value from 'revision_seq';")
	err := row.Scan(&rev)
	if err != nil {
		return err
	}
	
	go s.poll(ctx, s.notify, rev)

	return nil
}

// Start querying on interval and pumping into channel
func (s *PGStructured) poll(ctx context.Context, result chan interface{}, pollStart int64) {

	var (
		last        = pollStart
		//skipTime    time.Time
	)

	wait := time.NewTicker(time.Second)
	defer wait.Stop()
	defer close(result)


	for {
		select {
		case <-ctx.Done():
			return
		case <-wait.C:
		}

		rows, err := s.query(ctx, "SELECT * from listAll($1);", last)
		if err != nil {
			logrus.Errorf("Failed to list latest changes while watching: %v", err)
			continue
		}
		//TODO rework to be simpler?
		rev, _, events, err := RowsToEvents(rows)
		last = rev
		if err != nil {
			logrus.Errorf("fail to convert rows changes: %v", err)
			continue
		}

		if len(events) == 0 {
			continue
		}
		s.notify <- events
	}


	close(result)

}

//TODO pull channel into this file
func (s *PGStructured) Watch(ctx context.Context, prefix string, revision int64) <-chan []*server.Event {

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	result := make(chan []*server.Event, 100)
	logrus.Tracef("WATCH LIST key=%s rev=%d", prefix, revision)

	go func() {
		for i := range s.notify {
			result <- filter(i, true, prefix)
		}
		close(result)
		cancel()
	}()

	return result
}

func filter(events interface{}, checkPrefix bool, prefix string) []*server.Event {
	eventList := events.([]*server.Event)
	filteredEventList := make([]*server.Event, 0, len(eventList))

	for _, event := range eventList {
		if (checkPrefix && strings.HasPrefix(event.KV.Key, prefix)) || event.KV.Key == prefix {
			filteredEventList = append(filteredEventList, event)
		}
	}

	return filteredEventList
}


func (l *PGStructured) DbSize(ctx context.Context) (int64, error) {
	var size int64
	row := l.queryRow(ctx, fmt.Sprintf(`SELECT pg_database_size('%s');`, l.DatabaseName))
	if err := row.Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}




func (l *PGStructured) query(ctx context.Context, sql string, args ...interface{}) (result *sql.Rows, err error) {
	//logrus.Tracef("QUERY %v : %s", args, util.Stripped(sql))
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, ErrCode(err), util.Stripped(sql), args)
	}()
	return l.DB.QueryContext(ctx, sql, args...)
}

func (l *PGStructured) queryRow(ctx context.Context, sql string, args ...interface{}) (result *sql.Row) {
	//logrus.Tracef("QUERY ROW %v : %s", args, util.Stripped(sql))
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, ErrCode(result.Err()), util.Stripped(sql), args)
	}()
	return l.DB.QueryRowContext(ctx, sql, args...)
}

// func (l *PGStructured) execute(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {

// 	wait := strategy.Backoff(backoff.Linear(100 + time.Millisecond))
// 	for i := uint(0); i < 20; i++ {
// 		logrus.Tracef("EXEC (try: %d) %v : %s", i, args, util.Stripped(sql))
// 		startTime := time.Now()
// 		result, err = l.DB.ExecContext(ctx, sql, args...)
// 		metrics.ObserveSQL(startTime, ErrCode(err), util.Stripped(sql), args)
// 		if err != nil /*&& l.Retry != nil && l.Retry(err)*/ {
// 			wait(i)
// 			continue
// 		}
// 		return result, err
// 	}
// 	return
// }

// TODO move this back to pgsql.go ?
func ErrCode (err error) string {
	if err == nil {
		return ""
	}
	if err, ok := err.(*pq.Error); ok {
		return string(err.Code)
	}
	return err.Error()
}

func TranslateErr(err error) error {
	if err, ok := err.(*pq.Error); ok && err.Code == "23505" {
		return server.ErrKeyExists
	}
	return err
}


// For reference:
//
// type KeyValue struct {
// 	Key            string
// 	CreateRevision int64
// 	ModRevision    int64
// 	Value          []byte
// 	Lease          int64
// }

// id  | rev_key | theid |       name       | created | deleted | create_revision | prev_revision | lease |                value                 |              old_value               

func RowsToEvents(rows *sql.Rows) (int64, int64, []*server.Event, error) {
	var (
		result  []*server.Event
		rev     int64
		compact int64
	)
	defer rows.Close()

	for rows.Next() {
		event := &server.Event{}
		if err := scan(rows, &rev, &compact, event); err != nil {
			logrus.Errorf("Error Scanning in RowsToEvents, $v", err )
			return 0, 0, nil, err
		}
		result = append(result, event)
	}

	return rev, compact, result, nil
}

func scan(rows *sql.Rows, rev *int64, compact *int64, event *server.Event) error {
	event.KV = &server.KeyValue{}
	event.PrevKV = &server.KeyValue{}

	c := &sql.NullInt64{}

	err := rows.Scan(
		rev,
		c,
		&event.KV.ModRevision,
		&event.KV.Key,
		&event.Create,
		&event.Delete,
		&event.KV.CreateRevision,
		&event.PrevKV.ModRevision,
		&event.KV.Lease,
		&event.KV.Value,
		&event.PrevKV.Value,
	)
	if err != nil {
		return err
	}

	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	}

	*compact = c.Int64
	return nil
}
