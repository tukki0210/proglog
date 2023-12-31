package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	// オフセットは4バイト
	offWidth uint64 = 4
	// ファイルの位置は8バイト
	posWidth uint64 = 8
	// エントリの大きさ
	entWidth = offWidth + posWidth
)

type index struct {
	file *os.File
	// mmapは、ファイルの内容をメモリにマッピングする
	mmap gommap.MMap
	size uint64
}

// 新しくインデックスを作成する
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	// ファイルのメタ情報を取得する
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// ファイルのサイズを取得して、インデックスのサイズに設定する
	idx.size = uint64(fi.Size())

	// os.Truncate() 指定された名前のファイルサイズを、指定されたバイト数に切り詰める
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// インデックスを閉じる
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

// オフセットを受け取って、ストアからレコードの位置を取得する
func (i *index) Read(in int64)(out uint32, pos uint64, err error){
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / entWidth) -1 )
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth

	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}


func (i *index) Write(off uint32, pos uint64) error {
	// エントリを書き込むための領域があるかどうかを確認する
	if i.isMaxed() {
		return io.EOF
	}
	
	// 領域があれば、オフセットと位置をエンコードして、メモリにマップされたファイルに書き込む
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	i.size += uint64(entWidth)
	return nil
}

func (i *index) isMaxed() bool {
	return uint64(len(i.mmap)) < i.size+entWidth
}

func (i *index) Name() string {
	return i.file.Name()
}

