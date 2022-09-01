package server

import (
	"context"
	"database/sql"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

var (
	ErrKeyExists = rpctypes.ErrGRPCDuplicateKey
	ErrCompacted = rpctypes.ErrGRPCCompacted
)

type Backend interface {
	Start(ctx context.Context) error
	Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (int64, *KeyValue, error)
	Create(ctx context.Context, key string, value []byte, lease int64) (int64, error)
	Delete(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*KeyValue, error)
	Count(ctx context.Context, prefix string) (int64, int64, error)
	Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error)
	Watch(ctx context.Context, key string, revision int64) <-chan []*Event
	DbSize(ctx context.Context) (int64, error)
}

type Dialect interface {
	ListCurrent(ctx context.Context, prefix string, limit int64, includeDeleted bool) (*sql.Rows, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error)
	Count(ctx context.Context, prefix string) (int64, int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	After(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error)
	Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (int64, error)
	GetRevision(ctx context.Context, revision int64) (*sql.Rows, error)
	DeleteRevision(ctx context.Context, revision int64) error
	GetCompactRevision(ctx context.Context) (int64, error)
	SetCompactRevision(ctx context.Context, revision int64) error
	Compact(ctx context.Context, revision int64) (int64, error)
	PostCompact(ctx context.Context) error
	Fill(ctx context.Context, revision int64) error
	IsFill(key string) bool
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
	GetSize(ctx context.Context) (int64, error)
}

type Transaction interface {
	Commit() error
	MustCommit()
	Rollback() error
	MustRollback()
	GetCompactRevision(ctx context.Context) (int64, error)
	SetCompactRevision(ctx context.Context, revision int64) error
	Compact(ctx context.Context, revision int64) (int64, error)
	GetRevision(ctx context.Context, revision int64) (*sql.Rows, error)
	DeleteRevision(ctx context.Context, revision int64) error
	CurrentRevision(ctx context.Context) (int64, error)
}


// From: https://etcd.io/docs/v3.5/learning/api/#key-value-pair
//
// Key - key in bytes. An empty key is not allowed.
// Value - value in bytes.
// Version - version is the version of the key. A deletion resets the version to zero and any modification of the key increases its version.
// Create_Revision - revision of the last creation on the key.
// Mod_Revision - revision of the last modification on the key.
// Lease - the ID of the lease attached to the key. If lease is 0, then no lease is attached to the key.

type KeyValue struct {
	Key            string
	CreateRevision int64
	ModRevision    int64
	Value          []byte
	Lease          int64
}

// From: https://etcd.io/docs/v3.5/learning/api/#events
//
// Type - The kind of event. A PUT type indicates new data has been stored to the key. A DELETE indicates the key was deleted.
// KV - The KeyValue associated with the event. A PUT event contains current kv pair. A PUT event with kv.Version=1 indicates the creation of a key. A DELETE event contains the deleted key with its modification revision set to the revision of deletion.
// Prev_KV - The key-value pair for the key from the revision immediately before the event. To save bandwidth, it is only filled out if the watch has explicitly enabled it.

type Event struct {
	Delete bool
	Create bool
	KV     *KeyValue
	PrevKV *KeyValue
}
