package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

// 用于实现reader
type StandAloneReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := conf.DBPath + "/kv"
	raftPath := conf.DBPath + "/raft"
	kvEngine := engine_util.CreateDB(kvPath, false)
	var raftEngine *badger.DB
	if conf.Raft {
		raftEngine = engine_util.CreateDB(raftPath, true)
	}
	engines := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)

	return &StandAloneStorage{
		engines: engines,
		config:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	value := s.engines.Kv.NewTransaction(false)
	if value == nil {
		return nil, nil
	}
	reader := &StandAloneReader{
		kvTxn: value,
	}
	return reader, nil
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.kvTxn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.kvTxn)
}

func (s *StandAloneReader) Close() {
	s.kvTxn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			put := b.Data.(storage.Put)
			key := put.Key
			value := put.Value
			cf := put.Cf
			err := engine_util.PutCF(s.engines.Kv, cf, key, value)
			if err != nil {
				return err
			}
			break
		case storage.Delete:
			del := b.Data.(storage.Delete)
			key := del.Key
			cf := del.Cf
			err := engine_util.DeleteCF(s.engines.Kv, cf, key)
			if err != nil {
				return nil
			}
			break
		}
	}
	return nil
}
