package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"reflector-cleaner/atime"

	"github.com/google/gops/agent"
	"github.com/karrick/godirwalk"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/sirupsen/logrus"
)

var blobsDir string

func main() {
	if err := agent.Listen(agent.Options{}); err != nil {
		panic(err)
	}
	if len(os.Args) != 2 {
		panic("you must pass 1 argument: the path of the blobs directory")
	}
	blobsDir = os.Args[1]
	thresholdStr := os.Getenv("DISK_THRESHOLD")
	threshold := 0.90
	var err error
	if thresholdStr != "" {
		threshold, err = strconv.ParseFloat(thresholdStr, 64)
		if err != nil {
			panic(err)
		}
	}
	if _, err := os.Stat(blobsDir); os.IsNotExist(err) {
		panic(errors.Err("directory doesn't exist: %s", blobsDir))
	}
	used, _, err := getUsedSpace()
	if err != nil {
		logrus.Errorln(err.Error())
		return
	}
	logrus.Infof("disk usage: %.2f%%\n", used*100)
	if used > threshold {
		logrus.Infof("over %.2f%%, cleaning up", threshold*100)
		err = WipeOldestBlobs()
		if err != nil {
			logrus.Errorln(err.Error())
			return
		}
		usage, _, err := getUsedSpace()
		if err != nil {
			logrus.Errorln(err.Error())
			return
		}
		logrus.Infof("disk usage: %.2f%%\n", usage*100)
		logrus.Infoln("Done cleaning up")
	}
}

// GetUsedSpace returns a value between 0 and 1, with 0 being completely empty and 1 being full, for the disk that holds the provided path
func getUsedSpace() (usage float64, used uint64, err error) {
	var stat syscall.Statfs_t
	err = syscall.Statfs(blobsDir, &stat)
	if err != nil {
		return 0, 0, err
	}
	// Available blocks * size per block = available space in bytes
	all := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	used = all - free

	return float64(used) / float64(all), used, nil
}

func WipeOldestBlobs() (err error) {
	type datedFile struct {
		Atime    time.Time
		FullPath string
	}
	datedFiles := make([]datedFile, 0, 5000)
	checkedBlobs := int32(0)
	dirs, err := ioutil.ReadDir(blobsDir)
	if err != nil {
		return err
	}
	lock := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	startTime := time.Now()
	usage, used, err := getUsedSpace()
	if err != nil {
		logrus.Errorln(err.Error())
		return
	}
	for i, dir := range dirs {
		if (i+1)%(runtime.NumCPU()-1) == 0 {
			wg.Wait()
		}
		wg.Add(1)
		go func(dir string, lock *sync.Mutex, wg *sync.WaitGroup) {
			defer wg.Done()
			err = godirwalk.Walk(filepath.Join(blobsDir, dir), &godirwalk.Options{
				Callback: func(osPathname string, de *godirwalk.Dirent) error {
					if !de.IsDir() {
						i := atomic.AddInt32(&checkedBlobs, 1)
						if i%100 == 0 {
							logrus.Infof("checked %d blobs", i)
						}
						if i%100 == 0 {
							processedSize := float64(i * 2)
							speed := processedSize / time.Since(startTime).Seconds()
							eta, err := time.ParseDuration(fmt.Sprintf("%ds", int(float64(used)/1024./1024./speed)))
							if err != nil {
								return err
							}
							logrus.Infof("%.2f GB checked (speed: %.2f MB/s) ETA: %s", processedSize/1024.0, speed, eta.String())
						}
						if de.IsRegular() {
							stat, err := os.Stat(osPathname)
							if err != nil {
								return err
							}
							lock.Lock()
							datedFiles = append(datedFiles, datedFile{
								Atime:    atime.Atime(stat),
								FullPath: osPathname,
							})
							lock.Unlock()
						}
					}
					return nil
				},
				Unsorted: true, // (optional) set true for faster yet non-deterministic enumeration (see godoc)
			})
			if err != nil {
				logrus.Errorf(errors.FullTrace(err))
			}
		}(dir.Name(), lock, wg)
	}
	wg.Wait()
	sort.Slice(datedFiles, func(i, j int) bool {
		return datedFiles[i].Atime.Before(datedFiles[j].Atime)
	})

	spaceToFreeUp := ((float64(used) / usage) * 0.1) / 1024. / 1024.
	blobsToDelete := int(spaceToFreeUp / 2)
	logrus.Infof("spaceToFreeUp: %.2f MB - %d blobs", spaceToFreeUp, blobsToDelete)
	for i, df := range datedFiles {
		if i >= blobsToDelete {
			break
		}
		err = os.Remove(df.FullPath)
		if err != nil {
			return err
		}
	}
	return nil
}
