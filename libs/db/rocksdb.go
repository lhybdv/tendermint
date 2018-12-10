package db

import (
	"github.com/tecbot/gorocksdb"
	"bytes"
	"fmt"
	"path/filepath"

	cmn "github.com/tendermint/tendermint/libs/common"
)

var (
	openOpts  = gorocksdb.NewDefaultOptions()
	readOpts  = gorocksdb.NewDefaultReadOptions()
	writeOpts = gorocksdb.NewDefaultWriteOptions()
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewRocksDB(name, dir)
	}
	registerDBCreator(LevelDBBackend, dbCreator, false)
	registerDBCreator(RocksDBBackend, dbCreator, false)
}

var _ DB = (*RocksDB)(nil)

type RocksDB struct {
	db *gorocksdb.DB
}

func NewRocksDB(name string, dir string) (*RocksDB, error) {
    dbPath := filepath.Join(dir, name+".db")
    openOpts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(openOpts, dbPath)
	if err != nil {
		return nil, err
	}
	database := &RocksDB{
		db: db,
	}
	return database, nil
}

// Implements DB.
func (db *RocksDB) Get(key []byte) []byte {
	key = nonNilBytes(key)
	sl, err := db.db.Get(readOpts, key)
	if err != nil {
		panic(err)
	}
	return sl.Data()
}

// Implements DB.
func (db *RocksDB) Has(key []byte) bool {
	return db.Get(key) != nil
}

// Implements DB.
func (db *RocksDB) Set(key []byte, value []byte) {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	err := db.db.Put(writeOpts, key, value)
	if err != nil {
		cmn.PanicCrisis(err)
	}
}

// Implements DB.
func (db *RocksDB) SetSync(key []byte, value []byte) {
	key = nonNilBytes(key)
    value = nonNilBytes(value)
    opts := writeOpts
    opts.SetSync(true)
	err := db.db.Put(opts, key, value)
	if err != nil {
		cmn.PanicCrisis(err)
	}
}

// Implements DB.
func (db *RocksDB) Delete(key []byte) {
	key = nonNilBytes(key)
	err := db.db.Delete(writeOpts, key)
	if err != nil {
		cmn.PanicCrisis(err)
	}
}

// Implements DB.
func (db *RocksDB) DeleteSync(key []byte) {
	key = nonNilBytes(key)
    opts := writeOpts
    opts.SetSync(true)
	err := db.db.Delete(writeOpts, key)
	if err != nil {
		cmn.PanicCrisis(err)
	}
}

func (db *RocksDB) DB() *gorocksdb.DB {
	return db.db
}

// Implements DB.
func (db *RocksDB) Close() {
	db.db.Close()
}

// Implements DB.
func (db *RocksDB) Print() {
	str := db.db.GetProperty("leveldb.stats")
	fmt.Printf("%v\n", str)

    itr := db.db.NewIterator(readOpts)
    for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
}

// Implements DB.
func (db *RocksDB) Stats() map[string]string {
	// keys := []string{
	// 	"leveldb.num-files-at-level{n}",
	// 	"leveldb.stats",
	// 	"leveldb.sstables",
	// 	"leveldb.blockpool",
	// 	"leveldb.cachedblock",
	// 	"leveldb.openedtables",
	// 	"leveldb.alivesnaps",
	// 	"leveldb.aliveiters",
	// }

	stats := make(map[string]string)
	// for _, key := range keys {
	// 	str := db.db.GetProperty(key)
	// 	if err == nil {
	// 		stats[key] = str
	// 	}
	// }
	return stats
}

//----------------------------------------
// Batch

// Implements DB.
func (db *RocksDB) NewBatch() Batch {
    batch := gorocksdb.NewWriteBatch()
	return &rocksDBBatch{db, batch}
}

type rocksDBBatch struct {
	db    *RocksDB
	batch *gorocksdb.WriteBatch
}

// Implements Batch.
func (mBatch *rocksDBBatch) Set(key, value []byte) {
	mBatch.batch.Put(key, value)
}

// Implements Batch.
func (mBatch *rocksDBBatch) Delete(key []byte) {
	mBatch.batch.Delete(key)
}

// Implements Batch.
func (mBatch *rocksDBBatch) Write() {
	err := mBatch.db.db.Write(writeOpts, mBatch.batch)
	if err != nil {
		panic(err)
	}
}

// Implements Batch.
func (mBatch *rocksDBBatch) WriteSync() {
    opts := writeOpts
    opts.SetSync(true)
	err := mBatch.db.db.Write(opts, mBatch.batch)
	if err != nil {
		panic(err)
	}
}

//----------------------------------------
// Iterator
// NOTE This is almost identical to db/c_level_db.Iterator
// Before creating a third version, refactor.

// Implements DB.
func (db *RocksDB) Iterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(readOpts)
	return newRocksDBIterator(itr, start, end, false)
}

// Implements DB.
func (db *RocksDB) ReverseIterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(readOpts)
	return newRocksDBIterator(itr, start, end, true)
}

type rocksDBIterator struct {
	source    *gorocksdb.Iterator
	start     []byte
	end       []byte
	isReverse bool
	isInvalid bool
}

var _ Iterator = (*rocksDBIterator)(nil)

func newRocksDBIterator(source *gorocksdb.Iterator, start, end []byte, isReverse bool) *rocksDBIterator {
	if isReverse {
		if end == nil {
			source.SeekToLast()
		} else {
			source.Seek(end)
			if source.Valid() {
				eoakey := source.Key() // end or after key
				if bytes.Compare(end, eoakey.Data()) <= 0 {
					source.Prev()
				}
			} else {
				source.SeekToLast()
			}
		}
	} else {
		if start == nil {
			source.SeekToFirst()
		} else {
			source.Seek(start)
		}
	}
	return &rocksDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

// Implements Iterator.
func (itr *rocksDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Implements Iterator.
func (itr *rocksDBIterator) Valid() bool {

	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// Panic on DB error.  No way to recover.
	itr.assertNoError()

	// If source is invalid, invalid.
	if !itr.source.Valid() {
		itr.isInvalid = true
		return false
	}

	// If key is end or past it, invalid.
	var start = itr.start
	var end = itr.end
	var key = itr.source.Key()

	if itr.isReverse {
		if start != nil && bytes.Compare(key.Data(), start) < 0 {
			itr.isInvalid = true
			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key.Data()) <= 0 {
			itr.isInvalid = true
			return false
		}
	}

	// Valid
	return true
}

// Implements Iterator.
func (itr *rocksDBIterator) Key() []byte {
	// Key returns a copy of the current key.
	// See https://github.com/syndtr/goleveldb/blob/52c212e6c196a1404ea59592d3f1c227c9f034b2/leveldb/iterator/iter.go#L88
	itr.assertNoError()
	itr.assertIsValid()
	return cp(itr.source.Key().Data())
}

// Implements Iterator.
func (itr *rocksDBIterator) Value() []byte {
	// Value returns a copy of the current value.
	// See https://github.com/syndtr/goleveldb/blob/52c212e6c196a1404ea59592d3f1c227c9f034b2/leveldb/iterator/iter.go#L88
	itr.assertNoError()
	itr.assertIsValid()
	return cp(itr.source.Value().Data())
}

// Implements Iterator.
func (itr *rocksDBIterator) Next() {
	itr.assertNoError()
	itr.assertIsValid()
	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
}

// Implements Iterator.
func (itr *rocksDBIterator) Close() {
	itr.source.Close()
}

func (itr *rocksDBIterator) assertNoError() {
	if err := itr.source.Err(); err != nil {
		panic(err)
	}
}

func (itr rocksDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("rocksDBIterator is invalid")
	}
}
