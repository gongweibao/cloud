package pfsmodules

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
)

type ChunkMeta struct {
	Offset   int64  `json:"offset"`
	Checksum string `json:"checksum"`
	Len      int64  `json:"len"`
}

/*
type ChunkMetaCmdAttr struct {
	Path      string
	BlockSize uint32
}
*/

type ChunkMetaCmdResponse struct {
	Err   string      `json:"err"`
	Path  string      `json:"path"`
	Metas []ChunkMeta `json:"metas"`
}

func (p *ChunkMetaCmdResponse) SetErr(err string) {
	p.Err = err
}

func (p *ChunkMetaCmdResponse) GetErr() string {
	return p.Err
}

type ChunkMetaCmdAttr struct {
	Method    string `json:"method"`
	Path      string `json:"path"`
	ChunkSize int64  `json:"chunksize"`
}

type ChunkMetaCmd struct {
	cmdAttr *ChunkMetaCmdAttr
	resp    *ChunkMetaCmdResponse
}

func (p *ChunkMetaCmd) GetCmdAttr() *ChunkMetaCmdAttr {
	return p.cmdAttr
}

func (p *ChunkMetaCmd) GetResponse() *ChunkMetaCmdResponse {
	return p.resp
}

func (p *ChunkMetaCmd) SetResponse(resp *ChunkMetaCmdResponse) {
	p.resp = resp
}

func NewChunkMetaCmd(cmdAttr *ChunkMetaCmdAttr,
	resp *ChunkMetaCmdResponse) *ChunkMetaCmd {
	return &ChunkMetaCmd{
		cmdAttr: cmdAttr,
		resp:    resp,
	}
}

/*
type ChunkReq struct {
	Method    string
	Path      string
	ChunkSize int64
	Offset    int64
}
*/

func GetChunkRequest(r *http.Request) (*ChunkCmdAttr, error) {
	method := r.URL.Query().Get("method")
	path := r.URL.Query().Get("path")
	chunkStr := r.URL.Query().Get("chunksize")
	offsetStr := r.URL.Query().Get("offset")

	//resp := ChunkMetaCmdResponse{}
	if len(method) == 0 || len(path) < 4 {
		return nil, errors.New("check your params")
	}

	/*
		if method != "getchunkmeta" {
			return nil, errors.New(http.StatusText(http.StatusMethodNotAllowed))
		}
	*/

	if len(path) < 4 {
		return nil, errors.New("path error")
	}

	chunkSize := int64(DefaultChunkSize)
	if len(chunkStr) == 0 {
		chunkSize = DefaultChunkSize
	} else {
		inputSize, err := strconv.Atoi(chunkStr)
		if err != nil {
			return nil, errors.New("chunksize error")
		}
		chunkSize = int64(inputSize)
	}

	//if chunkSize < defaultMinChunkSize || chunkSize > defaultMaxChunkSize {
	if chunkSize > defaultMaxChunkSize {
		return nil, errors.New("chunksize error")
	}

	offset := int64(0)
	if len(chunkStr) == 0 {
	} else {
		inputSize, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return nil, errors.New("offset error")
		}
		offset = int64(inputSize)
	}

	return &ChunkCmdAttr{
		Method:    method,
		Path:      path,
		ChunkSize: chunkSize,
		Offset:    offset,
	}, nil
}

func GetChunkMetaCmd(w http.ResponseWriter, r *http.Request) *ChunkMetaCmd {
	method := r.URL.Query().Get("method")
	path := r.URL.Query().Get("path")
	chunkStr := r.URL.Query().Get("chunksize")

	//log.Println(method + path + chunkStr)

	resp := ChunkMetaCmdResponse{}
	if len(method) == 0 || len(path) < 4 {
		resp.SetErr("check your params")
		WriteCmdJsonResponse(w, &resp, http.StatusExpectationFailed)
		return nil
	}

	if method != "getchunkmeta" {
		resp.SetErr(http.StatusText(http.StatusMethodNotAllowed))
		WriteCmdJsonResponse(w, &resp, http.StatusMethodNotAllowed)
		return nil
	}

	if len(path) < 4 {
		resp.SetErr("path error")
		WriteCmdJsonResponse(w, &resp, http.StatusExpectationFailed)
		return nil
	}

	chunkSize := int64(DefaultChunkSize)
	if len(chunkStr) == 0 {
	} else {
		inputSize, err := strconv.Atoi(chunkStr)
		if err != nil {
			resp.SetErr("chunksize error")
			WriteCmdJsonResponse(w, &resp, http.StatusExpectationFailed)
			return nil
		}
		chunkSize = int64(inputSize)
	}

	if chunkSize < defaultMinChunkSize || chunkSize > defaultMaxChunkSize {
		resp.SetErr("chunksize error")
		WriteCmdJsonResponse(w, &resp, http.StatusExpectationFailed)
		return nil
	}

	cmdAttr := ChunkMetaCmdAttr{}
	cmdAttr.Method = method
	cmdAttr.Path = path
	cmdAttr.ChunkSize = chunkSize

	log.Println(cmdAttr)

	//cmd := ChunkMetaCmd{}
	return NewChunkMetaCmd(&cmdAttr, &resp)
}

func (p *ChunkMetaCmd) RunAndResponse(w http.ResponseWriter) {
	//c.Run()
	metas, err := GetChunksMeta(p.cmdAttr.Path, p.cmdAttr.ChunkSize)
	if err != nil {
		p.resp.SetErr(err.Error())
		WriteCmdJsonResponse(w, p.resp, http.StatusExpectationFailed)
		return
	}

	p.resp.Path = p.cmdAttr.Path
	p.resp.Metas = metas
	log.Println(len(metas))

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(p.resp); err != nil {
		//w.WriteHeader(http.StatusInternalServerError)
		log.Printf("write response error:%v", err)
		return
	}

	return
}

func GetChunksMeta(path string, len int64) ([]ChunkMeta, error) {
	f, err := os.Open(path) // For read access.
	if err != nil {
		return nil, err
	}

	defer f.Close()

	if len > defaultMaxChunkSize || len < defaultMinChunkSize {
		//len = defaultMaxChunkSize
		return nil, errors.New(BadChunkSizeArguments)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	metas := make([]ChunkMeta, 0, fi.Size()/int64(len)+1)
	data := make([]byte, len)
	offset := int64(0)

	for {
		n, err := f.Read(data)
		if err != nil && err != io.EOF {
			return metas, err
		}

		if err == io.EOF {
			break
		}

		log.Println(n)
		m := ChunkMeta{}
		m.Offset = offset
		sum := md5.Sum(data[:n])
		m.Checksum = hex.EncodeToString(sum[:])
		m.Len = int64(n)

		metas = append(metas, m)

		offset += int64(n)
	}

	//log.Println(len()
	return metas, nil
}

func (p *ChunkMetaCmdAttr) GetRequestUrl(urlPath string) (string, error) {
	var Url *url.URL
	Url, err := url.Parse(urlPath)
	if err != nil {
		return "", err
	}

	//log.Println(Url.Path)
	//Url.Path = urlPath + "/api/v1/chunks"
	//Url.Path = "/api/v1/chunks"
	parameters := url.Values{}
	parameters.Add("method", p.Method)
	parameters.Add("path", p.Path)

	str := fmt.Sprint(p.ChunkSize)
	parameters.Add("chunksize", str)

	Url.RawQuery = parameters.Encode()
	log.Println(Url.RawQuery)

	return urlPath + "/api/v1/chunks?" + Url.RawQuery, nil
}

type metaSlice []ChunkMeta

func (a metaSlice) Len() int           { return len(a) }
func (a metaSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a metaSlice) Less(i, j int) bool { return a[i].Offset < a[j].Offset }

func GetDiffChunksMeta(srcMeta []ChunkMeta, destMeta []ChunkMeta) ([]ChunkMeta, error) {
	if destMeta == nil || len(destMeta) == 0 || len(srcMeta) == 0 {
		return srcMeta, nil
	}

	if !sort.IsSorted(metaSlice(srcMeta)) {
		sort.Sort(metaSlice(srcMeta))
	}

	if !sort.IsSorted(metaSlice(destMeta)) {
		sort.Sort(metaSlice(destMeta))
	}

	dstIdx := 0
	srcIdx := 0
	diff := make([]ChunkMeta, 0, len(srcMeta))

	for {
		if srcMeta[srcIdx].Offset < destMeta[dstIdx].Offset {
			diff = append(diff, srcMeta[srcIdx])
			srcIdx += 1
		} else if srcMeta[srcIdx].Offset > destMeta[dstIdx].Offset {
			dstIdx += 1
		} else {
			if srcMeta[srcIdx].Checksum != destMeta[dstIdx].Checksum {
				diff = append(diff, srcMeta[srcIdx])
			}

			dstIdx += 1
			srcIdx += 1
		}

		if dstIdx >= len(destMeta) {
			break
		}

		if srcIdx >= len(srcMeta) {
			break
		}
	}

	diff = append(diff, srcMeta[srcIdx:len(srcMeta)]...)

	return nil, nil
}
