// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/coreos/pkg/capnslog"
)

var (
	defaultBatchLimit    = 10000
	defaultBatchInterval = 100 * time.Millisecond

	defragLimit = 10000

	// initialMmapSize is the initial size of the mmapped region. Setting this larger than
	// the potential max db size can prevent writer from blocking reader.
	// This only works for linux.
	initialMmapSize = uint64(10 * 1024 * 1024 * 1024)

	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "mvcc/backend")

	// minSnapshotWarningTimeout is the minimum threshold to trigger a long running snapshot warning.
	minSnapshotWarningTimeout = time.Duration(30 * time.Second)
)

// Backend 对外（store)提供数据库操作接口，内部实际调用boltDB的接口，是将底层存储和上层应用进行解耦的中间层，
// 如果有需要还可以跟换boltDB
type Backend interface {
	ReadTx() ReadTx   // 只读事务接口
	BatchTx() BatchTx // 批量读写事务接口

	Snapshot() Snapshot
	Hash(ignores map[IgnoreKey]struct{}) (uint32, error)
	// Size returns the current size of the backend.
	Size() int64
	// SizeInUse returns the current size of the backend logically in use.
	// Since the backend can manage free space in a non-byte unit such as
	// number of pages, the returned value can be not exactly accurate in bytes.
	SizeInUse() int64
	Defrag() error
	ForceCommit()
	Close() error
}

type Snapshot interface {
	// Size gets the size of the snapshot.
	Size() int64
	// WriteTo writes the snapshot into the given writer.
	WriteTo(w io.Writer) (n int64, err error)
	// Close closes the snapshot.
	Close() error
}

// 结构体backends是Backend接口的具体实现
type backend struct {
	// size and commits are used with atomic operations so they must be
	// 64-bit aligned, otherwise 32-bit tests will crash

	// size is the number of bytes in the backend
	size int64

	// sizeInUse is the number of bytes actually used in the backend
	sizeInUse int64

	// commits counts number of commits since start
	commits int64

	mu sync.RWMutex
	db *bolt.DB

	batchInterval time.Duration
	batchLimit    int
	batchTx       *batchTxBuffered

	readTx *readTx

	stopc chan struct{}
	donec chan struct{}
}

type BackendConfig struct {
	// Path is the file path to the backend file.
	Path string
	// BatchInterval is the maximum time before flushing the BatchTx.
	BatchInterval time.Duration
	// BatchLimit is the maximum puts before flushing the BatchTx.
	BatchLimit int
	// MmapSize is the number of bytes to mmap for the backend.
	MmapSize uint64
}

func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		BatchInterval: defaultBatchInterval,
		BatchLimit:    defaultBatchLimit,
		MmapSize:      initialMmapSize,
	}
}

func New(bcfg BackendConfig) Backend {
	return newBackend(bcfg)
}

func NewDefaultBackend(path string) Backend {
	bcfg := DefaultBackendConfig()
	bcfg.Path = path
	return newBackend(bcfg)
}

func newBackend(bcfg BackendConfig) *backend {
	bopts := &bolt.Options{}
	if boltOpenOptions != nil {
		*bopts = *boltOpenOptions
	}
	bopts.InitialMmapSize = bcfg.mmapSize() // 获取内存映射的大小

	db, err := bolt.Open(bcfg.Path, 0600, bopts) // 打开boltdb数据库
	if err != nil {
		plog.Panicf("cannot open database at %s (%v)", bcfg.Path, err)
	}

	// In future, may want to make buffering optional for low-concurrency systems
	// or dynamically swap between buffered/non-buffered depending on workload.
	b := &backend{
		db: db,

		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,

		readTx: &readTx{
			buf: txReadBuffer{
				txBuffer: txBuffer{make(map[string]*bucketBuffer)},
			},
			buckets: make(map[string]*bolt.Bucket),
		},

		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}
	b.batchTx = newBatchTxBuffered(b)
	go b.run()
	return b
}

// BatchTx returns the current batch tx in coalescer. The tx can be used for read and
// write operations. The write result can be retrieved within the same tx immediately.
// The write result is isolated with other txs until the current one get committed.
func (b *backend) BatchTx() BatchTx {
	return b.batchTx
}

func (b *backend) ReadTx() ReadTx { return b.readTx }

// ForceCommit forces the current batching tx to commit.
func (b *backend) ForceCommit() {
	b.batchTx.Commit()
}

func (b *backend) Snapshot() Snapshot { // 用当前DB中的数据创建相应的快照
	b.batchTx.Commit() // 将当前缓冲区的读写事务提交

	b.mu.RLock()
	defer b.mu.RUnlock()
	tx, err := b.db.Begin(false) // 开启一个只读事务
	if err != nil {
		plog.Fatalf("cannot begin tx (%s)", err)
	}

	stopc, donec := make(chan struct{}), make(chan struct{}) //用来提供快照数据是否完成发送的channel
	dbBytes := tx.Size()                                     // 获取整个DB保存的数据并记录数据大小
	go func() {                                              // 后台启动一个goroutine用来检测快照数据是否已经发送完成
		defer close(donec)
		// sendRateBytes is based on transferring snapshot data over a 1 gigabit/s connection
		// assuming a min tcp throughput of 100MB/s.
		var sendRateBytes int64 = 100 * 1024 * 1014
		warningTimeout := time.Duration(int64((float64(dbBytes) / float64(sendRateBytes)) * float64(time.Second)))
		if warningTimeout < minSnapshotWarningTimeout {
			warningTimeout = minSnapshotWarningTimeout
		}
		start := time.Now()
		ticker := time.NewTicker(warningTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C: // 超过预期最大发送时间则打印告警信息
				plog.Warningf("snapshotting is taking more than %v seconds to finish transferring %v MB [started at %v]", time.Since(start).Seconds(), float64(dbBytes)/float64(1024*1014), start)
			case <-stopc:
				snapshotDurations.Observe(time.Since(start).Seconds())
				return
			}
		}
	}()

	return &snapshot{tx, stopc, donec}
}

type IgnoreKey struct {
	Bucket string
	Key    string
}

func (b *backend) Hash(ignores map[IgnoreKey]struct{}) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	b.mu.RLock()
	defer b.mu.RUnlock()
	err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("cannot get hash of bucket %s", string(next))
			}
			h.Write(next)
			b.ForEach(func(k, v []byte) error {
				bk := IgnoreKey{Bucket: string(next), Key: string(k)}
				if _, ok := ignores[bk]; !ok {
					h.Write(k)
					h.Write(v)
				}
				return nil
			})
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

func (b *backend) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *backend) SizeInUse() int64 {
	return atomic.LoadInt64(&b.sizeInUse)
}

func (b *backend) run() { // 后台起一个gorotuine
	defer close(b.donec)
	t := time.NewTimer(b.batchInterval) // 定时提交批量读写数据
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-b.stopc:
			b.batchTx.CommitAndStop()
			return
		}
		b.batchTx.Commit()       // 定时器触发后将批量的读写数据提交，并开始一个新的事务
		t.Reset(b.batchInterval) // 重置定时器
	}
}

func (b *backend) Close() error {
	close(b.stopc)
	<-b.donec
	return b.db.Close()
}

// Commits returns total number of commits since start
func (b *backend) Commits() int64 {
	return atomic.LoadInt64(&b.commits)
}

func (b *backend) Defrag() error {
	return b.defrag()
}

func (b *backend) defrag() error {
	now := time.Now()

	// TODO: make this non-blocking?
	// lock batchTx to ensure nobody is using previous tx, and then
	// close previous ongoing tx.
	b.batchTx.Lock()
	defer b.batchTx.Unlock()

	// lock database after lock tx to avoid deadlock.
	b.mu.Lock()
	defer b.mu.Unlock()

	// block concurrent read requests while resetting tx
	b.readTx.mu.Lock()
	defer b.readTx.mu.Unlock()

	b.batchTx.unsafeCommit(true)
	b.batchTx.tx = nil

	tmpdb, err := bolt.Open(b.db.Path()+".tmp", 0600, boltOpenOptions)
	if err != nil {
		return err
	}

	err = defragdb(b.db, tmpdb, defragLimit)

	if err != nil {
		tmpdb.Close()
		os.RemoveAll(tmpdb.Path())
		return err
	}

	dbp := b.db.Path()
	tdbp := tmpdb.Path()

	err = b.db.Close()
	if err != nil {
		plog.Fatalf("cannot close database (%s)", err)
	}
	err = tmpdb.Close()
	if err != nil {
		plog.Fatalf("cannot close database (%s)", err)
	}
	err = os.Rename(tdbp, dbp)
	if err != nil {
		plog.Fatalf("cannot rename database (%s)", err)
	}

	b.db, err = bolt.Open(dbp, 0600, boltOpenOptions)
	if err != nil {
		plog.Panicf("cannot open database at %s (%v)", dbp, err)
	}
	b.batchTx.tx, err = b.db.Begin(true)
	if err != nil {
		plog.Fatalf("cannot begin tx (%s)", err)
	}

	b.readTx.reset()
	b.readTx.tx = b.unsafeBegin(false)

	size := b.readTx.tx.Size()
	db := b.db
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(db.Stats().FreePageN)*int64(db.Info().PageSize)))

	took := time.Since(now)
	defragDurations.Observe(took.Seconds())

	return nil
}

func defragdb(odb, tmpdb *bolt.DB, limit int) error {
	// open a tx on tmpdb for writes
	tmptx, err := tmpdb.Begin(true)
	if err != nil {
		return err
	}

	// open a tx on old db for read
	tx, err := odb.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	c := tx.Cursor()

	count := 0
	for next, _ := c.First(); next != nil; next, _ = c.Next() {
		b := tx.Bucket(next)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %s", string(next))
		}

		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		if berr != nil {
			return berr
		}
		tmpb.FillPercent = 0.9 // for seq write in for each

		b.ForEach(func(k, v []byte) error {
			count++
			if count > limit {
				err = tmptx.Commit()
				if err != nil {
					return err
				}
				tmptx, err = tmpdb.Begin(true)
				if err != nil {
					return err
				}
				tmpb = tmptx.Bucket(next)
				tmpb.FillPercent = 0.9 // for seq write in for each

				count = 0
			}
			return tmpb.Put(k, v)
		})
	}

	return tmptx.Commit()
}

func (b *backend) begin(write bool) *bolt.Tx {
	b.mu.RLock()
	tx := b.unsafeBegin(write)
	b.mu.RUnlock()

	size := tx.Size()
	db := tx.DB()
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(db.Stats().FreePageN)*int64(db.Info().PageSize)))

	return tx
}

func (b *backend) unsafeBegin(write bool) *bolt.Tx {
	tx, err := b.db.Begin(write)
	if err != nil {
		plog.Fatalf("cannot begin tx (%s)", err)
	}
	return tx
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(batchInterval time.Duration, batchLimit int) (*backend, string) {
	dir, err := ioutil.TempDir(os.TempDir(), "etcd_backend_test")
	if err != nil {
		plog.Fatal(err)
	}
	tmpPath := filepath.Join(dir, "database")
	bcfg := DefaultBackendConfig()
	bcfg.Path, bcfg.BatchInterval, bcfg.BatchLimit = tmpPath, batchInterval, batchLimit
	return newBackend(bcfg), tmpPath
}

func NewDefaultTmpBackend() (*backend, string) {
	return NewTmpBackend(defaultBatchInterval, defaultBatchLimit)
}

type snapshot struct {
	*bolt.Tx
	stopc chan struct{}
	donec chan struct{}
}

func (s *snapshot) Close() error {
	close(s.stopc)
	<-s.donec
	return s.Tx.Rollback()
}
