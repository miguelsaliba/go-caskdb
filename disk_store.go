package caskdb

import (
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"time"
)

// DiskStore is a Log-Structured Hash Table as described in the BitCask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's location on the disk.
//
// The idea is simple yet brilliant:
//   - Write the record to the disk
//   - Update the internal hash table to point to that byte offset
//   - Whenever we get a read request, check the internal hash table for the address,
//     fetch that and return
//
// KeyDir does not store values, only their locations.
//
// The above approach solves a lot of problems:
//   - Writes are insanely fast since you are just appending to the file
//   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
//     storage, there could be 2-3 disk seeks
//
// However, there are drawbacks too:
//   - We need to maintain an in-memory hash table KeyDir. A database with a large
//     number of keys would require more RAM
//   - Since we need to build the KeyDir at initialisation, it will affect the startup
//     time too
//   - Deleted keys need to be purged from the file to reduce the file size
//
// Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf
//
// DiskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing KV pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; till it is completed,
// we cannot use the database.
//
// Typical usage example:
//
//		store, _ := NewDiskStore("books.db")
//	   	store.Set("othello", "shakespeare")
//	   	author := store.Get("othello")
type DiskStore struct {
	file *os.File
	keyStore map[string]KeyEntry
}

func isFileExists(fileName string) bool {
	// https://stackoverflow.com/a/12518877
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	ds := &DiskStore{keyStore: make(map[string]KeyEntry)}
	if isFileExists(fileName) {
		err := ds.createKeyStore(fileName)
		if err != nil {
			log.Fatalln("Error creating keyStore", err)
		}
	}
	var err error
	ds.file, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Error creating/opening file", err)
	}
	return ds, nil
}

func (d *DiskStore) Get(key string) string {
	keyEntry, ok := d.keyStore[key]
	if !ok {
		return ""
	}

	_, err := d.file.Seek(int64(keyEntry.position), io.SeekStart)
	if err != nil {
		log.Fatal("Error seeking to value", err)
	}
	buf := make([]byte, keyEntry.totalSize)
	_, err = io.ReadFull(d.file, buf)
	if err != nil {
		log.Fatal("Error reading file", err)
	}

	_, _, value := decodeKV(buf)

	return value
}

func (d *DiskStore) Set(key string, value string) {
	timestamp := uint32(time.Now().Unix())
	size, bytes := encodeKV(timestamp, key, value)
	pos, err := d.file.Seek(0, io.SeekCurrent) // Get the current pos in the file
	if err != nil {
		log.Fatal("Failed to seek 0 positions, this should never happen", err)
	}
	_, err = d.file.Write(bytes)
	if err != nil {
		log.Fatal("Failed to write to file", err)
	}
	d.keyStore[key] = KeyEntry{timestamp, uint32(pos), uint32(size)}
}

func (d *DiskStore) Close() bool {
	err := d.file.Close()
	if err != nil {
		log.Print("Failed to close file", err)
		return false
	}
	return true
}

func (d *DiskStore) createKeyStore(fileName string) error {
	file, _ := os.Open(fileName)
	defer file.Close()

	for {
		buf := make([]byte, headerSize)
		pos, _ := file.Seek(0, io.SeekCurrent)
		// Read header
		_, err := io.ReadFull(file, buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("Could not read header ", err)
		}
		timestamp, keySize, valueSize := decodeHeader(buf)
		// Read key
		keyBuf := make([]byte, keySize)
		_, err = io.ReadFull(file, keyBuf)
		if err != nil {
			log.Fatal("Could not read key from file ", err)
		}
		// Skip value (not used)
		_, err = file.Seek(int64(valueSize), io.SeekCurrent)
		if err != nil && err != io.EOF {
			log.Fatalln("Could not skip value in file", err)
		}
		totalSize := headerSize + keySize + valueSize
		d.keyStore[string(keyBuf)] = KeyEntry{timestamp, uint32(pos), totalSize}
	}
	return nil
}
