package pgsql

import (
	"context"
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


//Takes:
// prefikeyx -- RangeRequest.Key
// startKey -- RangeRequest.RangeEnd 
// limit -- # of rows to return
// revision -- version being selected    -- We will ignore this for now and always return the latest. In future, can filter and send ErrCompacted if not found?
//Returns: 
// revRet -- current revision
// kvRet -- single matching keyvalue
func (l *PGStructured) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	defer func() {
		logrus.Tracef("GET %s, rev=%d, kv=%v, err=%v", key, revRet, kvRet != nil, errRet)
	}()

	rows, err := l.query(ctx, "SELECT * FROM list($1,$2)", key, limit)
	if err != nil {
		return 0, nil, err
	}

	rev, events, err := RowsToEvents(rows)
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

	return rev, events[0].KV, err
}

func (l *PGStructured) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	defer func() {
		logrus.Tracef("CREATE %s, size=%d, lease=%d => rev=%d, err=%v", key, len(value), lease, revRet, errRet)
	}()

	row := l.queryRow(ctx, "SELECT id FROM upsert($1, $2, $3);", key, lease, value)
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

	row := l.queryRow(ctx, "SELECT id, key, value FROM markDeleted($1);", key)
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


//Takes:
// prefix -- modified RangeRequest.RangeEnd
// startKey -- trimmed RangeRequest.Key
// limit -- # of rows to return
// revision -- version being selected    -- We will ignore this for now and always return the latest. In future, can filter and send ErrCompacted if not found?
//Returns: 
// revRet -- current revision
// kvRet -- all of the keyvalues in range
func (l *PGStructured) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {

	logrus.Tracef("LIST %s limit=%d, startKey=%s ", prefix, limit, startKey)

	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	prefix += "%"


	rows, err := l.query(ctx, "SELECT * FROM list($1, $2);", prefix, limit)
	if err != nil {
		return 0,nil,err
	}
	revRet, events, err:= RowsToEvents(rows)
	if err != nil {
		logrus.Errorf("fail to convert rows when listing: %v", err)
		return 0,nil,err
	}

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

	// defer func() {
	// 	logrus.Tracef("COUNT %s => rev=%d, count=%d, err=%v", prefix, revRet, count, err)
	// }()

	if strings.HasSuffix(prefix, "/") {
		prefix += "%"
	}

	row := l.queryRow(ctx, "SELECT curr_rev, count FROM countkeys($1);", prefix)
	err = row.Scan(&revRet, &count)
	return revRet, count, err
}

func (l *PGStructured) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	var (
		prev_revision int64
	)
	// defer func() {
	// 	logrus.Tracef("UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, updated=%v, err=%v", key, len(value), revision, lease, revRet, updateRet, errRet)
	// }()

	row := l.queryRow(ctx, "SELECT id, prev_revision FROM upsert($1, $2, $3);", key, lease, value)
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
	
	row := s.queryRow(ctx, "SELECT last_value from revision_seq;")
	err := row.Scan(&rev)
	if err != nil {
		logrus.Errorf("ERROR while starting poll: %", err)
		return err
	}
	
	go s.poll(ctx, rev)

	return nil
}

// Start querying on interval and pumping into channel
func (s *PGStructured) poll(ctx context.Context, pollStart int64) {

	var (
		last        = pollStart
		//skipTime    time.Time
	)

	wait := time.NewTicker(time.Second)
	defer wait.Stop()

	for {
		select {
		// case <-ctx.Done():
		// 	logrus.Tracef("POLL ENDING")
		// 	return
		case <-wait.C:
		}

		rows, err := s.query(ctx, "SELECT * from listAllSince($1);", last)
		if err != nil {
			logrus.Errorf("Failed to list latest changes while watching: %v", err)
			continue
		}
		
		rev, events, err := RowsToEvents(rows)
		if err != nil {
			logrus.Errorf("fail to convert rows changes: %v", err)
			continue
		}

		if len(events) == 0 {
			continue
		}

		s.notify <- events
		
		last = rev
	}


}


func (s *PGStructured) startSub() (chan interface{}, error){
	return s.notify, nil
}

func (s *PGStructured) Watch(ctx context.Context, prefix string, revision int64) <-chan []*server.Event {

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	result := make(chan []*server.Event, 100)

	watcher, err := s.broadcaster.Subscribe(ctx, s.startSub)

	if err != nil {
		logrus.Tracef("WATCH SUB err=%v", err)
	}

	go func() {
		for i := range watcher {
			result <- filter(i, prefix)
		}
		close(result)
		cancel()
	}()

	return result
}

func filter(events interface{}, prefix string) []*server.Event {
	eventList := events.([]*server.Event)
	filteredEventList := make([]*server.Event, 0, len(eventList))

	for _, event := range eventList {
		if strings.HasPrefix(event.KV.Key, prefix) || event.KV.Key == prefix {
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

func RowsToEvents(rows *sql.Rows) (int64, []*server.Event, error) {
	var (
		result  []*server.Event
		rev     int64
	)
	defer rows.Close()

	for rows.Next() {
		event := &server.Event{}
		event.KV = &server.KeyValue{}
		event.PrevKV = &server.KeyValue{}

		// curr_rev,
		// kv.id AS theid, 
		// kv.name, 
		// kv.created, 
		// kv.deleted, 
		// coalesce(kv.create_revision,0), 
		// coalesce(kv.prev_revision, kv.id), 
		// kv.lease, 
		// kv.value, 
		// kv.old_value
		err := rows.Scan(
			&rev,
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
			logrus.Errorf("Error Scanning in RowsToEvents, $v", err)
			return 0, nil, err
		}

		if event.Create {
			event.KV.CreateRevision = event.KV.ModRevision
			event.PrevKV = nil
		}

		result = append(result, event)
	}

	return rev, result, nil
}