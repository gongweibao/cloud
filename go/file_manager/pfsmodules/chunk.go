package pfsmodules

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cloud/go/file_manager/pfscommon"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
)

const (
	defaultMaxChunkSize = 4 * 1024 * 1024
	defaultMinChunkSize = 4 * 1024
	DefaultChunkSize    = 2 * 1024 * 1024
)

type Chunk struct {
	Meta ChunkMeta
	Data []byte
}

type ChunkCmdAttr struct {
	Method    string `json:"method"`
	Path      string `json:"path"`
	Offset    int64  `json:"offset"`
	ChunkSize int64  `chunksize`
}

type ChunkCmd struct {
	cmdAttr *ChunkCmdAttr
	resp    *Chunk
}

func FromArgs(method string, path string, offset int64, chunkSize int64) *ChunkCmdAttr {
	return &ChunkCmdAttr{
		Method:    method,
		Path:      path,
		Offset:    offset,
		ChunkSize: chunkSize,
	}
}

func (p *ChunkCmd) GetCmdAttr() *ChunkCmdAttr {
	return p.cmdAttr
}

func NewChunkCmdAttr(r *http.Request) (*ChunkCmdAttr, error) {

	body, err := ioutil.ReadAll(io.LimitReader(r.Body, MaxJsonRequestSize))
	if err != nil {
		return nil, err
	}

	if err := r.Body.Close(); err != nil {
		return nil, err
	}

	c := &ChunkCmdAttr{}
	if err := json.Unmarshal(body, c); err != nil {
		return nil, err
	}
	return c, nil
}

func GetChunkWriter(path string, offset int64) (*os.File, error) {
	fd, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	return fd, nil
}

func GetChunk(path string, offset int64, len uint32) (*Chunk, error) {
	f, err := os.Open(path) // For read access.
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if len > defaultMaxChunkSize || len < defaultMinChunkSize {
		return nil, errors.New("invalid len:" + string(len))
	}

	chunk := Chunk{}
	chunk.Data = make([]byte, len)
	m := &chunk.Meta

	if _, err := f.Seek(offset, os.SEEK_SET); err != nil {
		return nil, err
	}

	n, err := f.Read(chunk.Data)
	if err != nil && err != io.EOF {
		return nil, err
	}

	m.Offset = offset
	sum := md5.Sum(chunk.Data[:n])
	m.Checksum = hex.EncodeToString(sum[:])
	m.Len = int64(n)

	return &chunk, nil
}

func GetFileNameParam(path string, offset int64, len int64) string {
	parameters := url.Values{}
	parameters.Add("filename", path)

	str := fmt.Sprint(offset)
	parameters.Add("offset", str)

	str = fmt.Sprint(len)
	parameters.Add("chunksize", str)

	//Url.RawQuery = parameters.Encode()

	return parameters.Encode()
	//return fmt.Sprintf("filename=%s&offset=%d&chunksize=%d", path, offset, len)
}

// path example:
// 	  filename=/pfs/datacenter1/1.txt&offset=4096&chunksize=4096
func ParseFileNameParam(path string) (*ChunkCmdAttr, error) {
	attr := ChunkCmdAttr{}

	m, err := url.ParseQuery(path)
	log.Printf("%v\n", m)
	if err != nil ||
		len(m["filename"]) == 0 ||
		len(m["offset"]) == 0 ||
		len(m["chunksize"]) == 0 {
		return &attr, errors.New(http.StatusText(http.StatusBadGateway))
	}

	//var err error
	attr.Path = m["filename"][0]
	attr.Offset, err = strconv.ParseInt(m["offset"][0], 10, 64)
	if err != nil {
		return &attr, errors.New("bad arguments offset")
	}

	chunkSize, err := strconv.ParseInt(m["chunksize"][0], 10, 64)
	if err != nil {
		return &attr, errors.New("bad arguments offset")
	}
	attr.ChunkSize = chunkSize

	return &attr, nil
}

func (p *ChunkCmdAttr) GetRequestUrl(uri string, path string) (string, error) {
	parameters := url.Values{}
	parameters.Add("method", p.Method)
	parameters.Add("path", p.Path)

	str := fmt.Sprint(p.Offset)
	parameters.Add("offset", str)

	str = fmt.Sprint(p.ChunkSize)
	parameters.Add("chunksize", str)

	return fmt.Sprintf("%s/%s?%s", uri, path, parameters.Encode()), nil
}

//func writeStreamChunkData(path string, offset int64, len int64, w http.ResponseWriter) error {
func WriteStreamChunkData(path string, offset int64, len int64, w io.Writer) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(offset, 0)
	if err != nil {
		return err
	}

	writer := multipart.NewWriter(w)
	defer writer.Close()

	writer.SetBoundary(pfscommon.MultiPartBoundary)

	fileName := GetFileNameParam(path, offset, len)
	log.Printf("fileName:%s\n", fileName)
	part, err := writer.CreateFormFile("chunk", fileName)
	if err != nil {
		return err
	}

	_, err = io.CopyN(part, file, len)
	if err != nil {
		return err
	}
	return nil
}
