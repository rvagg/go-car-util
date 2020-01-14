package main

// similar to https://github.com/ipfs/go-car/blob/master/car/main.go but with
// small reads and seeks so as to not consume entire block contents for indexing

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
	"github.com/urfave/cli"
)

var cidv0Prefix = []byte{0x12, 0x20}

type CarHeader struct {
	Roots   []cid.Cid `json:"roots"`
	Version uint64    `json:"version"`
}

type BlockEntry struct {
	Cid         cid.Cid `json:"cid"`
	Offset      int     `json:"offset"`
	Length      int     `json:"length"`
	BlockOffset int     `json:"blockOffset"`
	BlockLength int     `json:"blockLength"`
}

func init() {
	cbor.RegisterCborType(CarHeader{})
}

// binary.Uvarint() but without advancing the reader, returns the uint as well as
// the number of bytes it's encoded in
func readUvarint(rd *bufio.Reader, offset int) (uint64, int, error) {
	varintBuf, err := rd.Peek(7 + offset)
	if err != nil {
		return 0, 0, err
	}
	if len(varintBuf) != 7+offset {
		return 0, 0, fmt.Errorf("Bad CAR format: not enough bytes for next block")
	}
	if offset > 0 {
		varintBuf = varintBuf[offset:]
	}
	varint, varintBytes := binary.Uvarint(varintBuf)

	return varint, varintBytes, nil
}

// read the length-prefix, just a uvarint
func readLength(rd *bufio.Reader) (int, int, error) {
	length, lengthBytes, err := readUvarint(rd, 0)
	if err != nil {
		return 0, 0, err
	}
	rd.Discard(lengthBytes)

	if length == 0 {
		return 0, 0, fmt.Errorf("Bad CAR format: got length 0 block")
	}
	return int(length), lengthBytes, nil
}

// mostly copied from go-car/util/util.go but reusing an existing reader
// and returning the number of bytes consumed by reading the CID
func readCid(rd *bufio.Reader) (cid.Cid, int, error) {
	v0Buf, err := rd.Peek(2)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	if bytes.Equal(v0Buf, cidv0Prefix) {
		// CIDv0
		v0Buf = make([]byte, 34)
		bytesRead, err := rd.Read(v0Buf)
		if err != nil {
			return cid.Cid{}, 0, err
		}
		if bytesRead != 34 {
			return cid.Cid{}, 0, fmt.Errorf("Bad CAR format: not enough bytes for CIDv0")
		}
		c, err := cid.Cast(v0Buf)
		return c, 34, err
	}

	// assume cidv1
	version, versionBytes, err := readUvarint(rd, 0)
	if err != nil {
		return cid.Cid{}, 0, err
	}
	rd.Discard(versionBytes)

	if version != 1 {
		return cid.Cid{}, 0, fmt.Errorf("Bad CAR format: invalid CID version number: %v", version)
	}

	codec, codecBytes, err := readUvarint(rd, 0)
	if err != nil {
		return cid.Cid{}, 0, err
	}
	rd.Discard(codecBytes)

	// peek into the multihash and read the two varints at the front to understand
	// how much data the multihash consumes, don't consume those bytes
	_, mhCodeBytes, err := readUvarint(rd, 0)
	mhLength, mhLengthBytes, err := readUvarint(rd, mhCodeBytes)

	mhr := mh.NewReader(rd)
	h, err := mhr.ReadMultihash() // defer the multihash reading to the multihash library
	if err != nil {
		return cid.Cid{}, 0, err
	}

	length := versionBytes + codecBytes + mhCodeBytes + mhLengthBytes + int(mhLength)

	return cid.NewCidV1(codec, h), length, nil
}

// GenerateCarIndex parses a CAR file and emits `BlockEntry` objects via the callback
// function for each entry it finds. Each entry consists of a CID, an offset for the
// start of the entry, length for the entire entry, an offset for the start of the
// block binary data and a length for the block binary data. The block offset and
// length can be used to seek and read individual blocks in a CAR.
// See also the `ls` command in https://github.com/ipfs/go-car/blob/master/car/main.go
func GenerateCarIndex(file string, cb func(BlockEntry) error) error {
	fi, err := os.Open(file)
	if err != nil {
		return err
	}
	defer fi.Close()

	var offset int = 0
	header := true

	for {
		rd := bufio.NewReaderSize(fi, 64) // TODO: how big can a CID be? is 64 enough?

		_, err = rd.Peek(1)
		if err == io.EOF {
			// normal end to parse, we did a seek to the end and there's nothing more
			break
		}
		if err != nil {
			return err
		}

		length, lengthBytes, err := readLength(rd)
		if err != nil {
			return err
		}

		var cidBytes int = 0
		if !header {
			var cid cid.Cid
			cid, cidBytes, err = readCid(rd)
			if err != nil {
				return err
			}

			entry := BlockEntry{
				Cid:         cid,
				Offset:      offset,
				Length:      lengthBytes + length,
				BlockOffset: offset + lengthBytes + cidBytes,
				BlockLength: length - cidBytes,
			}

			err = cb(entry)
			if err != nil {
				return err
			}
		} else {
			header = false
		}

		seekTo := offset + lengthBytes + length
		newOffset, err := fi.Seek(int64(seekTo), os.SEEK_SET)
		if err != nil {
			return err
		}

		if newOffset != int64(seekTo) {
			return fmt.Errorf("Bad CAR format: couldn't seek to correct position, truncated?")
		}

		offset = seekTo
	}

	return nil
}

// ParseCarHeader reads and parses only the header of a CAR file, returning a
// `CarHeader` object containing its roots and version.
// See also the `header` command in https://github.com/ipfs/go-car/blob/master/car/main.go
func ParseCarHeader(file string) (CarHeader, error) {
	fi, err := os.Open(file)
	if err != nil {
		return CarHeader{}, err
	}
	defer fi.Close()

	rd := bufio.NewReader(fi)

	length, _, err := readLength(rd)
	if err != nil {
		return CarHeader{}, err
	}

	var header CarHeader
	headerBuf := make([]byte, length)
	headerBytes, err := io.ReadFull(rd, headerBuf)
	if err != nil {
		return CarHeader{}, err
	}
	if headerBytes != length {
		return CarHeader{}, fmt.Errorf("Bad CAR format: couldn't read full header")
	}
	if err := cbor.DecodeInto(headerBuf, &header); err != nil {
		return CarHeader{}, err
	}

	return header, nil
}

// print a line-delimited JSON index of the CAR file
func indexAction(c *cli.Context) error {
	return GenerateCarIndex(c.Args().First(), func(entry BlockEntry) error {
		b, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	})
}

// print the header of the CAR file as JSON
func headerAction(c *cli.Context) error {
	header, err := ParseCarHeader(c.Args().First())
	if err != nil {
		return err
	}
	b, err := json.Marshal(header)
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

var index = cli.Command{
	Name:        "index",
	Description: "Generate an index for a CAR file, print to stdout as line-delimited JSON",
	Action:      indexAction,
}

var header = cli.Command{
	Name:        "header",
	Description: "Print the header for a CAR file as JSON",
	Action:      headerAction,
}

func main() {
	app := cli.NewApp()
	app.Commands = []cli.Command{
		header,
		index,
	}
	app.RunAndExitOnError()
}
