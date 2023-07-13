package log

import (
	"fmt"
	"os"
	"path/filepath"

	api "github.com/tukki0210/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store      *store
	index      *index
	baseOffset uint64
	nextOffset uint64
	config     Config
}

// 新しいセグメントを作成する
// segment構造体の初期化関数を定義している
// 初期化関数はしばしばポインタを返す仕様とする
// この関数は、ディレクトリ、ベースオフセット、設定を受け取り、
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {

	// ファイルを作成する
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)

	if err != nil {
		return nil, err
	}

	// ストアを作成する
	if s.stroe, err := s.newStore(storeFile); err != nil {
		return nil, err
	}

	// インデックスファイルを作成する
	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0600,
	)

	if err != nil {
		return nil, err
	}

	// インデックスを作成する
	if s.index, err := s.newIndex(indexFile, c); err != nil {
		return nil, err
	}
	// インデックスを読み込む
	if err := s.index.Read(s.store); err != nil {
		return nil, err
	}
	// 次のオフセットを計算する
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// セグメントにレコードを追加する
func (s *segment) Append(record *api.Record) (offset uint64, err error){
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	if err := s.index.Write(
		uint32(s.nextOffset - s.baseOffset),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// セグメントからレコードを読み込む
func (s *segment) Read(off uint64) (*api.Record, error){
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)

	return record, nil
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes || 
		s.index.isMaxed()
}

// セグメントを閉じて、インデックスとストアのファイルを削除する
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// セグメントを閉じる
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}
