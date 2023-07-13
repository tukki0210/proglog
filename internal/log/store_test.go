package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	// test data
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T){
	f, err := os.CreateTemp("","store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	// append the test data to the store
	testAppend(t, s)
	testRead(t,s)
	testReadAt(t,s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t,s)
}

func testAppend(t *testing.T, s *store){
	t.Helper()

	for i := uint64(1); i<4; i++ {
		// ストアにデータを追加する
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store){
	t.Helper()
	var pos uint64

	// ストアから読み込んだデータが正しいか確認する
	for i := uint64(1); i<4; i++ {
		// ストアからデータを読み込む
		read, err := s.Read(pos)
		// エラーが発生しないことを確認する
		require.NoError(t, err)
		// 読み込んだデータが正しいことを確認する
		require.Equal(t, write, read)

		// 次の読み込み位置を計算する
		pos += width
	}
}

func testReadAt(t *testing.T, s *store){
	t.Helper()

	for i, off := uint64(1), int64(0); i < 4; i++ {
		

		b := make([]byte, lenWidth)
		// ストアからデータを読み込む
		n, err := s.ReadAt(b, off)
		// エラーが発生しないことを確認する
		require.NoError(t, err)
		// 読み込んだデータが正しいことを確認する
		require.Equal(t, lenWidth, n)
		

		off += int64(n)

		// ストアからデータを読み込む
		size := enc.Uint64(b)
		b = make([]byte, size)
		// 
		n, err = s.ReadAt(b, off)
		// エラーが発生しないことを確認する
		require.NoError(t, err)
		// 読み込んだデータが正しいことを確認する
		require.Equal(t, write, b)
		// 読み込んだバイト数が正しいことを確認する
		require.Equal(t, int(size), n)

		off += int64(n)
	}
}

func testClose(t *testing.T){
	f, err := os.CreateTemp("","store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	// ストアにデータを追加する
	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	// ストアを閉じる
	err = s.Close()
	require.NoError(t, err)

	// ストアにデータを追加しようとする
	_, afterSize, err := s.Append(write)
	require.Error(t, err)
	require.True(t, afterSize > uint64(beforeSize))
	
}

func openFile(name string)(file *os.File, size int64, err error){
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)

	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()

	if err != nil {
		return nil, 0, err
	}

	return f, fi.Size(), nil
}