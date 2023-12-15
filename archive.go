package levelar

import (
	"os"

	"github.com/emptyhua/saar"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func OpenArchive(path string) (*leveldb.DB, error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	ar := saar.NewReader(fp)

	fs := &arStorage{
		ar: ar,
	}

	return leveldb.Open(fs, &opt.Options{ReadOnly: true})
}

func CreateArchive(lvp string, arp string) error {
	progFunc := func(p1, p2 string, err error) {
	}
	return saar.CreateArchive(progFunc, arp, lvp)
}
