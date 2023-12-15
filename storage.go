package levelar

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/emptyhua/saar"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	errReadOnly = errors.New("leveldb/storage: storage is read-only")
)

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type arLock struct {
}

func (al *arLock) Unlock() {
}

// fileStorage is a file-system backed storage.
type arStorage struct {
	ar *saar.Reader
	mu sync.Mutex
}

func (fs *arStorage) Lock() (storage.Locker, error) {
	return &arLock{}, nil
}

func (fs *arStorage) Log(str string) {
}

func (fs *arStorage) log(str string) {
}

func (fs *arStorage) SetMeta(fd storage.FileDesc) error {
	return errReadOnly
}

func (fs *arStorage) GetMeta() (rtf storage.FileDesc, rterr error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	defer func() {
		fmt.Printf("rtf %s %+v\n", rtf.Type, rterr)
	}()

	hdrs, err := fs.ar.List()
	if err != nil {
		return storage.FileDesc{}, err
	}

	// Try this in order:
	// - CURRENT.[0-9]+ ('pending rename' file, descending order)
	// - CURRENT
	// - CURRENT.bak
	//
	// Skip corrupted file or file that point to a missing target file.
	type currentFile struct {
		name string
		fd   storage.FileDesc
	}
	tryCurrent := func(name string) (*currentFile, error) {
		fp, err := fs.ar.Open(name)
		if err != nil {
			if errors.Is(err, saar.ErrNotExist) {
				err = os.ErrNotExist
			}
			return nil, err
		}

		b, err := ioutil.ReadAll(fp)
		if err != nil {
			return nil, err
		}

		var fd storage.FileDesc
		if len(b) < 1 || b[len(b)-1] != '\n' || !fsParseNamePtr(string(b[:len(b)-1]), &fd) {
			fs.log(fmt.Sprintf("%s: corrupted content: %q", name, b))
			err := &storage.ErrCorrupted{
				Err: errors.New("leveldb/storage: corrupted or incomplete CURRENT file"),
			}
			return nil, err
		}

		if _, err := fs.ar.Open(fsGenName(fd)); err != nil {
			if errors.Is(err, saar.ErrNotExist) {
				fs.log(fmt.Sprintf("%s: missing target file: %s", name, fd))
				err = os.ErrNotExist
			}
			return nil, err
		}

		return &currentFile{name: name, fd: fd}, nil
	}

	tryCurrents := func(names []string) (*currentFile, error) {
		var (
			cur *currentFile
			// Last corruption error.
			lastCerr error
		)
		for _, name := range names {
			var err error
			cur, err = tryCurrent(name)
			if err == nil {
				break
			} else if err == os.ErrNotExist {
				// Fallback to the next file.
			} else if isCorrupted(err) {
				lastCerr = err
				// Fallback to the next file.
			} else {
				// In case the error is due to permission, etc.
				return nil, err
			}
		}
		if cur == nil {
			err := os.ErrNotExist
			if lastCerr != nil {
				err = lastCerr
			}
			return nil, err
		}
		return cur, nil
	}

	// Try 'pending rename' files.
	var nums []int64
	for _, hdr := range hdrs {
		name := hdr.Path
		if strings.HasPrefix(name, "CURRENT.") && name != "CURRENT.bak" {
			i, err := strconv.ParseInt(name[8:], 10, 64)
			if err == nil {
				nums = append(nums, i)
			}
		}
	}
	var (
		pendCur   *currentFile
		pendErr   = os.ErrNotExist
		pendNames []string
	)
	if len(nums) > 0 {
		sort.Sort(sort.Reverse(int64Slice(nums)))
		pendNames = make([]string, len(nums))
		for i, num := range nums {
			pendNames[i] = fmt.Sprintf("CURRENT.%d", num)
		}
		pendCur, pendErr = tryCurrents(pendNames)
		if pendErr != nil && pendErr != os.ErrNotExist && !isCorrupted(pendErr) {
			return storage.FileDesc{}, pendErr
		}
	}

	// Try CURRENT and CURRENT.bak.
	curCur, curErr := tryCurrents([]string{"CURRENT", "CURRENT.bak"})
	if curErr != nil && curErr != os.ErrNotExist && !isCorrupted(curErr) {
		return storage.FileDesc{}, curErr
	}

	// pendCur takes precedence, but guards against obsolete pendCur.
	if pendCur != nil && (curCur == nil || pendCur.fd.Num > curCur.fd.Num) {
		curCur = pendCur
	}

	if curCur != nil {
		return curCur.fd, nil
	}

	// Nothing found.
	if isCorrupted(pendErr) {
		return storage.FileDesc{}, pendErr
	}

	return storage.FileDesc{}, curErr
}

func (fs *arStorage) List(ft storage.FileType) (fds []storage.FileDesc, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	hdrs, err := fs.ar.List()
	if err != nil {
		return nil, err
	}

	for _, hdr := range hdrs {
		name := hdr.Path
		if fd, ok := fsParseName(name); ok && fd.Type&ft != 0 {
			fds = append(fds, fd)
		}
	}

	return
}

func (fs *arStorage) Open(fd storage.FileDesc) (storage.Reader, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	of, err := fs.ar.Open(fsGenName(fd))
	if err != nil {
		if fsHasOldName(fd) && errors.Is(err, saar.ErrNotExist) {
			of, err = fs.ar.Open(fsGenOldName(fd))
			if err == nil {
				goto ok
			}
		}

		if errors.Is(err, saar.ErrNotExist) {
			err = os.ErrNotExist
		}

		return nil, err
	}
ok:
	return &fileWrap{FileReader: of}, nil
}

func (fs *arStorage) Create(fd storage.FileDesc) (storage.Writer, error) {
	return nil, errReadOnly
}

func (fs *arStorage) Remove(fd storage.FileDesc) error {
	return errReadOnly
}

func (fs *arStorage) Rename(oldfd, newfd storage.FileDesc) error {
	return errReadOnly
}

func (fs *arStorage) Close() error {
	return fs.ar.Close()
}

type fileWrap struct {
	*saar.FileReader
}

func (fw *fileWrap) Close() error {
	return nil
}

func fsGenName(fd storage.FileDesc) string {
	switch fd.Type {
	case storage.TypeManifest:
		return fmt.Sprintf("MANIFEST-%06d", fd.Num)
	case storage.TypeJournal:
		return fmt.Sprintf("%06d.log", fd.Num)
	case storage.TypeTable:
		return fmt.Sprintf("%06d.ldb", fd.Num)
	case storage.TypeTemp:
		return fmt.Sprintf("%06d.tmp", fd.Num)
	default:
		panic(fmt.Sprintf("invalid file type %v", fd.Type))
	}
}

func fsHasOldName(fd storage.FileDesc) bool {
	return fd.Type == storage.TypeTable
}

func fsGenOldName(fd storage.FileDesc) string {
	switch fd.Type {
	case storage.TypeTable:
		return fmt.Sprintf("%06d.sst", fd.Num)
	default:
		return fsGenName(fd)
	}
}

func fsParseName(name string) (fd storage.FileDesc, ok bool) {
	var tail string
	_, err := fmt.Sscanf(name, "%d.%s", &fd.Num, &tail)
	if err == nil {
		switch tail {
		case "log":
			fd.Type = storage.TypeJournal
		case "ldb", "sst":
			fd.Type = storage.TypeTable
		case "tmp":
			fd.Type = storage.TypeTemp
		default:
			return
		}
		return fd, true
	}
	n, _ := fmt.Sscanf(name, "MANIFEST-%d%s", &fd.Num, &tail)
	if n == 1 {
		fd.Type = storage.TypeManifest
		return fd, true
	}
	return
}

func fsParseNamePtr(name string, fd *storage.FileDesc) bool {
	_fd, ok := fsParseName(name)
	if fd != nil {
		*fd = _fd
	}
	return ok
}

func isCorrupted(err error) bool {
	switch err.(type) {
	case *storage.ErrCorrupted:
		return true
	default:
		return false
	}
}
