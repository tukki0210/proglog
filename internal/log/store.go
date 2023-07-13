package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

const (
	lenWidth = 8
)

var (
	enc = binary.BigEndian
)

// ストアは、ログの内容をファイルに書き込む
type store struct {
	*os.File
	mu sync.Mutex
	buf *bufio.Writer
	size uint64
}

// 新しくストアを作成する
func newStore(f *os.File) (*store, error){

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// get the size of the file
	size := uint64(fi.Size())

	// return a new store struct
	return &store{
		File: f,
		size: size,
		buf: bufio.NewWriter(f),
	}, nil
}

// ストアにデータを追加する
func (s *store) Append(p []byte)(n uint64, pos uint64, err error){

	// ロックを取得する
	s.mu.Lock()
	defer s.mu.Unlock()

	// ファイルの位置を取得する
	pos = s.size

	//　バイトスライスの長さを書き込む
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// write the byte slice to the file
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// increment the size of the file
	w += lenWidth

	// increment the size of the file
	s.size += uint64(w)

	// return the size of the file and the position of the file
	return uint64(w), pos, nil
	
}

// ストアからデータを読み込む
func (s *store) Read(pos uint64) ([]byte, error){
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush:流す バッファに溜まっているデータを強制的に書き出す
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// ストアからデータを読み込む
func (s *store) ReadAt(p []byte, off int64) (int, error){
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error{
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}

