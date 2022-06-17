package store

import (
	"bufio"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/xkeyideal/dragonboat-example/v3/config"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Store struct {
	shardId uint64
	path    string
	opts    PebbleShardOption
	log     *zap.Logger
	db      *pebble.DB
	ro      *pebble.IterOptions
	wo      *pebble.WriteOptions
}

func NewStore(shardId uint64, path string, opts PebbleShardOption, log *zap.Logger) (*Store, error) {
	cfg := getDefaultPebbleDBConfig()

	db, err := openPebbleDB(cfg, filepath.Join(path, "current"), opts, log)
	if err != nil {
		return nil, err
	}

	return &Store{
		shardId: shardId,
		log:     log,
		opts:    opts,
		db:      db,
		ro:      &pebble.IterOptions{},
		wo:      &pebble.WriteOptions{Sync: false},
		path:    path,
	}, nil
}

func (s *Store) GetColumnFamily(cf string) byte {
	return config.PebbleColumnFamilyMap[cf]
}

func (s *Store) GetBytes(key []byte) ([]byte, error) {
	val, closer, err := s.db.Get(key)

	// query key not found, return nil
	if err == pebble.ErrNotFound {
		return []byte{}, nil
	}

	if err != nil {
		return nil, err
	}

	// must be copy
	data := make([]byte, len(val))
	copy(data, val)

	if err := closer.Close(); err != nil {
		return nil, err
	}

	return data, nil
}

func (s *Store) BuildColumnFamilyKey(cf byte, key []byte) []byte {
	return append([]byte{cf}, key...)
}

func (s *Store) GetWo() *pebble.WriteOptions {
	return s.wo
}

func (s *Store) Batch() *pebble.Batch {
	return s.db.NewBatch()
}

func (s *Store) Write(b *pebble.Batch) error {
	return b.Commit(s.wo)
}

func (s *Store) GetIterator() *pebble.Iterator {
	return s.db.NewIter(s.ro)
}

func (s *Store) NewSnapshotDir() (string, error) {
	if !pathIsExist(s.path) {
		if err := os.MkdirAll(s.path, os.ModePerm); err != nil {
			return "", err
		}
	}

	path := filepath.Join(s.path, uuid.New().String(), string(os.PathSeparator))

	err := s.db.Checkpoint(path)
	return path, err
}

func (s *Store) SaveSnapShotToWriter(path string, writer io.Writer, stopChan <-chan struct{}) error {
	files := getAllFile(path, path)
	for _, f := range files {
		if isStop(stopChan) {
			return nil
		}

		err := readFileToWriter(path, f, writer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) LoadSnapShotFromReader(reader io.Reader, stopChan <-chan struct{}) error {
	rocksdbPath := filepath.Join(s.path, uuid.New().String())
	dataPath := filepath.Join(rocksdbPath, "data")
	newReader := bufio.NewReaderSize(reader, 4*1024*1024)

	for {
		if isStop(stopChan) {
			return nil
		}

		fileName, err := newReader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}

		fileName = strings.Trim(fileName, "\n")
		fileSizeByte := make([]byte, 8)
		_, err = io.ReadFull(newReader, fileSizeByte)
		if err != nil {
			return err
		}

		fileSize := binary.BigEndian.Uint64(fileSizeByte)
		filePath := filepath.Join(dataPath, fileName)
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}

		file, err := os.Create(filePath)
		if err != nil {
			return err
		}
		_, err = io.CopyN(file, newReader, int64(fileSize))
		_ = file.Close()
		if err != nil {
			return err
		}
	}

	if err := s.Close(); err != nil {
		return err
	}

	currentPath := filepath.Join(s.path, "current")
	if err := os.RemoveAll(currentPath); err != nil {
		return err
	}
	if err := os.Rename(rocksdbPath, currentPath); err != nil {
		return err
	}

	db, err := openPebbleDB(getDefaultPebbleDBConfig(), currentPath, s.opts, s.log)
	if err != nil {
		return err
	}

	s.db = db
	return nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}

	s.log.Sync()

	if s.db != nil {
		s.db.Flush()
		s.db.Close()
		s.db = nil
	}

	return nil
}

func isStop(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func getAllFile(basepath, pathname string) []*fileInfo {
	fileSlice := []*fileInfo{}

	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		panic("read directory error")
	}

	for _, fi := range rd {
		if fi.IsDir() {
			slice := getAllFile(basepath, pathname+"/"+fi.Name())
			fileSlice = append(fileSlice, slice...)
		} else {
			info := &fileInfo{
				FullName: strings.TrimPrefix(filepath.Join(pathname, fi.Name()), basepath),
				Size:     fi.Size(),
			}
			fileSlice = append(fileSlice, info)
		}
	}

	return fileSlice
}

type fileInfo struct {
	FullName string
	Size     int64
}

func readFileToWriter(path string, f *fileInfo, writer io.Writer) error {
	//打开文件
	file, err := os.Open(filepath.Join(path, f.FullName))
	if err != nil {
		return err
	}
	defer file.Close()

	//read file name
	var fileName = []byte(f.FullName)
	fileName = append(fileName, '\n')
	if err := writeTo(fileName, writer); err != nil {
		return err
	}

	//read file size
	var fileSize = make([]byte, 8)
	binary.BigEndian.PutUint64(fileSize, uint64(f.Size))
	if err := writeTo(fileSize, writer); err != nil {
		return err
	}

	// read file content
	_, err = io.Copy(writer, file)

	return err
}

func writeTo(bytes []byte, writer io.Writer) error {
	size := len(bytes)
	writeSize := 0

	for {
		if size-writeSize == 0 {
			return nil
		}
		n, err := writer.Write(bytes[writeSize:])
		if err != nil {
			return err
		}
		writeSize += n
	}
}

func pathIsExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		return false
	}
	return true
}
