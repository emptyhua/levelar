package levelar

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestArchive(t *testing.T) {
	dir, err := os.MkdirTemp("", "levelar*")
	t.Log(dir)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	lvp := filepath.Join(dir, "lv")
	lvdb, err := leveldb.OpenFile(lvp, nil)
	assert.Nil(t, err)

	err = lvdb.Put([]byte("k1"), []byte("v1"), nil)
	assert.Nil(t, err)

	err = lvdb.Close()
	assert.Nil(t, err)

	arp := lvp + ".ar"
	err = CreateArchive(lvp, arp)
	assert.Nil(t, err)

	lvdb, err = OpenArchive(arp)
	assert.Nil(t, err)

	v, err := lvdb.Get([]byte("k1"), nil)
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("v1"))

	err = lvdb.Close()
	assert.Nil(t, err)
}
