/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
)

var erasureEncodeDecodeTests = []struct {
	dataBlocks, parityBlocks   int
	missingData, missingParity int
	reconstructParity          bool
	shouldFail                 bool
}{
	{dataBlocks: 2, parityBlocks: 2, missingData: 0, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 3, parityBlocks: 3, missingData: 1, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 4, parityBlocks: 4, missingData: 2, missingParity: 0, reconstructParity: false, shouldFail: false},
	{dataBlocks: 5, parityBlocks: 5, missingData: 0, missingParity: 1, reconstructParity: true, shouldFail: false},
	{dataBlocks: 6, parityBlocks: 6, missingData: 0, missingParity: 2, reconstructParity: true, shouldFail: false},
	{dataBlocks: 7, parityBlocks: 7, missingData: 1, missingParity: 1, reconstructParity: false, shouldFail: false},
	{dataBlocks: 8, parityBlocks: 8, missingData: 3, missingParity: 2, reconstructParity: false, shouldFail: false},
	{dataBlocks: 2, parityBlocks: 2, missingData: 2, missingParity: 1, reconstructParity: true, shouldFail: true},
	{dataBlocks: 4, parityBlocks: 2, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: true},
	{dataBlocks: 8, parityBlocks: 4, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: false},
}

func TestReplicationDisk(t *testing.T) {
	path := "/root/data/rdata.txt"
	size := 1 //KB
	data := make([]byte, 0)
	fi, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	for i := 0; i < size; i++ {
		buf := make([]byte, 1024) //每次需要再for循环里面重新make，踩坑
		n, err := fi.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if 0 == n {
			break
		}
		data = append(data, buf...)
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			newPath := path + "-" + strconv.Itoa(i)
			f, err := os.Create(newPath)
			defer f.Close()
			if err != nil {
				t.Fatalf("Test %d: failed to create erasure: %v", i, err)
			}
			_, err = f.Write(data)
			if err != nil {
				log.Println("writeFile error ..err =", err)
				return
			}
			defer wg.Done()
		}()
	}
	wg.Wait()
}
func TestECDisk(t *testing.T) {
	path := "/root/data/rdata.txt"
	size := 1 //KB
	data := make([]byte, 0)
	fi, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	for i := 0; i < size; i++ {
		buf := make([]byte, 1024) //每次需要再for循环里面重新make，踩坑
		n, err := fi.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if 0 == n {
			break
		}
		data = append(data, buf...)
	}

	for i, test := range erasureEncodeDecodeTests {
		buffer := make([]byte, len(data), 2*len(data))
		copy(buffer, data)

		erasure, err := NewErasure(context.Background(), test.dataBlocks, test.parityBlocks, blockSizeV1)
		if err != nil {
			t.Fatalf("Test %d: failed to create erasure: %v", i, err)
		}
		encoded, err := erasure.EncodeData(context.Background(), buffer)
		if err != nil {
			t.Fatalf("Test %d: failed to encode data: %v", i, err)
		}
		blocks := len(encoded)
		for j := 0; j < blocks; j++ {
			newPath := path + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
			f, err := os.Create(newPath)
			defer f.Close()
			if err != nil {
				t.Fatalf("Test %d-%d: failed to create erasure: %v", i, j, err)
			}
			_, err = f.Write(encoded[j])
			if err != nil {
				log.Println("writeFile error ..err =", err)
				return
			}
		}
		for j := range encoded[:test.missingData] {
			encoded[j] = nil
		}
		for j := test.dataBlocks; j < test.dataBlocks+test.missingParity; j++ {
			encoded[j] = nil
		}

		if test.reconstructParity {
			err = erasure.DecodeDataAndParityBlocks(context.Background(), encoded)
		} else {
			err = erasure.DecodeDataBlocks(encoded)
		}

		if err == nil && test.shouldFail {
			t.Errorf("Test %d: test should fail but it passed", i)
		}
		if err != nil && !test.shouldFail {
			t.Errorf("Test %d: test should pass but it failed: %v", i, err)
		}

		decoded := encoded
		if !test.shouldFail {
			if test.reconstructParity {
				for j := range decoded {
					if decoded[j] == nil {
						t.Errorf("Test %d: failed to reconstruct shard %d", i, j)
					}
				}
			} else {
				for j := range decoded[:test.dataBlocks] {
					if decoded[j] == nil {
						t.Errorf("Test %d: failed to reconstruct data shard %d", i, j)
					}
				}
			}

			decodedData := new(bytes.Buffer)
			if _, err = writeDataBlocks(context.Background(), decodedData, decoded, test.dataBlocks, 0, int64(len(data))); err != nil {
				t.Errorf("Test %d: failed to write data blocks: %v", i, err)
			}
			if !bytes.Equal(decodedData.Bytes(), data) {
				t.Errorf("Test %d: Decoded data does not match original data: got: %v want: %v", i, decodedData.Bytes(), data)
			}
		}
	}
}
func TestCreateData(t *testing.T) {
	data := make([]byte, 1024*1024*1024)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		t.Fatalf("Failed to read random data: %v", err)
	}
	path := "/Users/lisongsong/Documents/rdata.txt"
	fi, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	_, err = fi.Write(data)
	if err != nil {
		log.Println("writeFile error ..err =", err)
		return
	}
}
func TestErasureEncodeDecode(t *testing.T) {
	data := make([]byte, 1024*1024)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		t.Fatalf("Failed to read random data: %v", err)
	}
	for i, test := range erasureEncodeDecodeTests {
		buffer := make([]byte, len(data), 2*len(data))
		copy(buffer, data)

		erasure, err := NewErasure(context.Background(), test.dataBlocks, test.parityBlocks, blockSizeV1)
		if err != nil {
			t.Fatalf("Test %d: failed to create erasure: %v", i, err)
		}
		encoded, err := erasure.EncodeData(context.Background(), buffer)
		if err != nil {
			t.Fatalf("Test %d: failed to encode data: %v", i, err)
		}

		for j := range encoded[:test.missingData] {
			encoded[j] = nil
		}
		for j := test.dataBlocks; j < test.dataBlocks+test.missingParity; j++ {
			encoded[j] = nil
		}

		if test.reconstructParity {
			err = erasure.DecodeDataAndParityBlocks(context.Background(), encoded)
		} else {
			err = erasure.DecodeDataBlocks(encoded)
		}

		if err == nil && test.shouldFail {
			t.Errorf("Test %d: test should fail but it passed", i)
		}
		if err != nil && !test.shouldFail {
			t.Errorf("Test %d: test should pass but it failed: %v", i, err)
		}

		decoded := encoded
		if !test.shouldFail {
			if test.reconstructParity {
				for j := range decoded {
					if decoded[j] == nil {
						t.Errorf("Test %d: failed to reconstruct shard %d", i, j)
					}
				}
			} else {
				for j := range decoded[:test.dataBlocks] {
					if decoded[j] == nil {
						t.Errorf("Test %d: failed to reconstruct data shard %d", i, j)
					}
				}
			}

			decodedData := new(bytes.Buffer)
			if _, err = writeDataBlocks(context.Background(), decodedData, decoded, test.dataBlocks, 0, int64(len(data))); err != nil {
				t.Errorf("Test %d: failed to write data blocks: %v", i, err)
			}
			if !bytes.Equal(decodedData.Bytes(), data) {
				t.Errorf("Test %d: Decoded data does not match original data: got: %v want: %v", i, decodedData.Bytes(), data)
			}
		}
	}
}

// Setup for erasureCreateFile and erasureReadFile tests.
type erasureTestSetup struct {
	dataBlocks   int
	parityBlocks int
	blockSize    int64
	diskPaths    []string
	disks        []StorageAPI
}

// Removes the temporary disk directories.
func (e erasureTestSetup) Remove() {
	for _, path := range e.diskPaths {
		os.RemoveAll(path)
	}
}

// Returns an initialized setup for erasure tests.
func newErasureTestSetup(dataBlocks int, parityBlocks int, blockSize int64) (*erasureTestSetup, error) {
	diskPaths := make([]string, dataBlocks+parityBlocks)
	disks := make([]StorageAPI, len(diskPaths))
	var err error
	for i := range diskPaths {
		disks[i], diskPaths[i], err = newPosixTestSetup()
		if err != nil {
			return nil, err
		}
		err = disks[i].MakeVol("testbucket")
		if err != nil {
			return nil, err
		}
	}
	return &erasureTestSetup{dataBlocks, parityBlocks, blockSize, diskPaths, disks}, nil
}
