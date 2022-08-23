package pgsql

import (
	"context"
	"sync"
	"time"
	"database/sql"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/metrics"
)

type Log interface {
	Start(ctx context.Context) error
	CurrentRevision(ctx context.Context) (int64, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeletes bool) (int64, []*server.Event, error)
	After(ctx context.Context, prefix string, revision, limit int64) (int64, []*server.Event, error)
	Watch(ctx context.Context, prefix string) <-chan []*server.Event
	Count(ctx context.Context, prefix string) (int64, int64, error)
	Append(ctx context.Context, event *server.Event) (int64, error)
	DbSize(ctx context.Context) (int64, error)
}

type PGStructured struct {
	DB *sql.DB
	DatabaseName string
}


func (l *PGStructured) Start(ctx context.Context) error {
	//TODO Start "compaction" (really just deleting what's marked as deleted)


	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if _, err := l.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			logrus.Errorf("Failed to create health check key: %v", err)
		}
	}
	go l.ttl(ctx)
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

	select {
		case l.notify <- revRet:
		default:
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

	select {
		case l.notify <- revRet:
		default:
	}
	
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
	defer func() {
		logrus.Tracef("LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v", prefix, startKey, limit, revision, revRet, len(kvRet), errRet)
	}()


	rows, err := l.query(ctx, "SELECT * FROM list($1, $2, $3, $4);", prefix, limit, startKey, revision)
	if err != nil {
		return 0,nil,err
	}
	revRet, _, events, err:= RowsToEvents(rows)
	if err != nil {
		return 0,nil,err
	}

	kvRet := make([]*server.KeyValue, 0, len(events))
	for _, event := range events {
		kvRet = append(kvs, event.KV)
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
	err := row.Scan(&revRet, &count)
	return revRet, count, nil
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

//TODO what is this?
func (l *PGStructured) ttlEvents(ctx context.Context) chan *server.Event {
	result := make(chan *server.Event)
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		wg.Wait()
		close(result)
	}()

	go func() {
		defer wg.Done()
		rev, events, err := l.log.List(ctx, "/", "", 1000, 0, false)
		for len(events) > 0 {
			if err != nil {
				logrus.Errorf("failed to read old events for ttl")
				return
			}

			for _, event := range events {
				if event.KV.Lease > 0 {
					result <- event
				}
			}

			_, events, err = l.log.List(ctx, "/", events[len(events)-1].KV.Key, 1000, rev, false)
		}
	}()

	go func() {
		defer wg.Done()
		for events := range l.log.Watch(ctx, "/") {
			for _, event := range events {
				if event.KV.Lease > 0 {
					result <- event
				}
			}
		}
	}()

	return result
}

func (l *PGStructured) ttl(ctx context.Context) {
	// vary naive TTL support
	mutex := &sync.Mutex{}
	for event := range l.ttlEvents(ctx) {
		go func(event *server.Event) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(event.KV.Lease) * time.Second):
			}
			mutex.Lock()
			if _, _, _, err := l.Delete(ctx, event.KV.Key, event.KV.ModRevision); err != nil {
				logrus.Errorf("failed to delete expired key: %v", err)
			}
			mutex.Unlock()
		}(event)
	}
}

//TODO pull channel into this file
func (l *PGStructured) Watch(ctx context.Context, prefix string, revision int64) <-chan []*server.Event {
	logrus.Tracef("WATCH %s, revision=%d", prefix, revision)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan := l.log.Watch(ctx, prefix)

	// include the current revision in list
	if revision > 0 {
		revision--
	}

	result := make(chan []*server.Event, 100)

	rev, kvs, err := l.log.After(ctx, prefix, revision, 0)
	if err != nil {
		logrus.Errorf("failed to list %s for revision %d", prefix, revision)
		cancel()
	}

	logrus.Tracef("WATCH LIST key=%s rev=%d => rev=%d kvs=%d", prefix, revision, rev, len(kvs))

	go func() {
		lastRevision := revision
		if len(kvs) > 0 {
			lastRevision = rev
		}

		if len(kvs) > 0 {
			result <- kvs
		}

		// always ensure we fully read the channel
		for i := range readChan {
			result <- filter(i, lastRevision)
		}
		close(result)
		cancel()
	}()

	return result
}

func filter(events []*server.Event, rev int64) []*server.Event {
	for len(events) > 0 && events[0].KV.ModRevision <= rev {
		events = events[1:]
	}

	return events
}

func (l *PGStructured) DbSize(ctx context.Context) (int64, error) {
	var size int64
	row := d.queryRow(ctx, fmt.Sprintf(`SELECT pg_database_size('%s');`, l.DatabaseName))
	if err := row.Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}




func (l *PGStructured) query(ctx context.Context, sql string, args ...interface{}) (result *sql.Rows, err error) {
	logrus.Tracef("QUERY %v : %s", args, util.Stripped(sql))
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, ErrCode(err), util.Stripped(sql), args)
	}()
	return l.DB.QueryContext(ctx, sql, args...)
}

func (l *PGStructured) queryRow(ctx context.Context, sql string, args ...interface{}) (result *sql.Row) {
	logrus.Tracef("QUERY ROW %v : %s", args, util.Stripped(sql))
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, ErrCode(result.Err()), util.Stripped(sql), args)
	}()
	return l.DB.QueryRowContext(ctx, sql, args...)
}

func (l *PGStructured) execute(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	if l.LockWrites {
		l.Lock()
		defer l.Unlock()
	}

	wait := strategy.Backoff(backoff.Linear(100 + time.Millisecond))
	for i := uint(0); i < 20; i++ {
		logrus.Tracef("EXEC (try: %d) %v : %s", i, args, util.Stripped(sql))
		startTime := time.Now()
		result, err = l.DB.ExecContext(ctx, sql, args...)
		metrics.ObserveSQL(startTime, ErrCode(err), util.Stripped(sql), args)
		// if err != nil && l.Retry != nil && l.Retry(err) {
		// 	wait(i)
		// 	continue
		// }
		return result, err
	}
	return
}

func ErrCode (err error) string {
	if err == nil {
		return ""
	}
	if err, ok := err.(*pq.Error); ok {
		return string(err.Code)
	}
	return err.Error()
}

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
			return 0, 0, nil, err
		}
		result = append(result, event)
	}

	return rev, compact, result, nil
}
