package main

import (
	"context"
	"flag"
	//"fmt"
	//"errors"
	"errors"
	"fmt"
	"github.com/cloud/go/file_manager/pfsmodules"
	"github.com/google/subcommands"
	"log"
	"os"
	"path/filepath"
	//"strings"
)

type cpCommand struct {
	v bool
}

func (*cpCommand) Name() string     { return "cp" }
func (*cpCommand) Synopsis() string { return "uoload or download files" }
func (*cpCommand) Usage() string {
	return `cp [-v] <src> <dest>
	upload or downlod files, does't support directories this version
	Options:
	`
}

func (p *cpCommand) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&p.v, "v", false, "Cause cp to be verbose, showing files after they are copied.")
}

func (p *cpCommand) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if f.NArg() < 2 {
		f.Usage()
		return subcommands.ExitFailure
	}

	attrs := pfsmodules.NewCpCmdAttr("cp", f)

	results, err := RunCp(attrs)
	if err != nil {
		return subcommands.ExitFailure
	}

	log.Println(results)

	return subcommands.ExitSuccess
}

func RunCp(p *pfsmodules.CpCmdAttr) ([]pfsmodules.CpCmdResult, error) {
	src, err := p.GetSrc()
	if err != nil {
		return nil, err
	}

	dest, err := p.GetDest()
	if err != nil {
		return nil, err
	}

	var results, ret []pfsmodules.CpCmdResult
	//return nil, nil
	log.Println(src)
	log.Println(dest)

	for _, arg := range src {
		log.Printf("ls %s\n", arg)

		if pfsmodules.IsRemotePath(arg) {
			if pfsmodules.IsRemotePath(dest) {
				//remotecp
				//ret, err = RemoteCp(p, arg, dest)
				m := pfsmodules.CpCmdResult{}
				m.Err = pfsmodules.OnlySupportUploadOrDownloadFiles
				m.Src = arg
				m.Dest = dest

				ret = append(ret, m)
			} else {
				//download
				ret, err = Download(arg, dest)
			}
		} else {
			if pfsmodules.IsRemotePath(dest) {
				//upload
				ret, err = Upload(arg, dest)
			} else {
				//can't do that
				m := pfsmodules.CpCmdResult{}
				m.Err = pfsmodules.CopyFromLocalToLocal
				m.Src = arg
				m.Dest = dest

				ret = append(ret, m)
			}
		}

		results = append(results, ret...)
	}

	return results, nil
}

func GetRemoteChunksMeta(path string, chunkSize int64) ([]pfsmodules.ChunkMeta, error) {
	cmdAttr := pfsmodules.ChunkMetaCmdAttr{
		Method:    "getchunkmeta",
		Path:      path,
		ChunkSize: chunkSize,
	}
	resp := pfsmodules.ChunkMetaCmdResponse{}
	s := NewCmdSubmitter(UserHomeDir() + "/.paddle/config")

	cmd := pfsmodules.NewChunkMetaCmd(&cmdAttr, &resp)
	err := s.SubmitChunkMetaRequest(8080, cmd)
	if err != nil {
		log.Printf("error: %v\n", err)
		return resp.Metas, err
	}

	//log.Printf("remote chunk meta ")
	//log.Println(resp)
	return resp.Metas, err
}

func DownloadChunks(src string, dest string, diffMeta []pfsmodules.ChunkMeta) error {
	if len(diffMeta) == 0 {
		log.Printf("srcfile:%s and destfile:%s are same\n", src, dest)
		return nil
	}

	s := NewCmdSubmitter(UserHomeDir() + "/.paddle/config")

	for _, meta := range diffMeta {
		cmdAttr := pfsmodules.FromArgs("getchunkdata", src, meta.Offset, meta.Len)
		err := s.GetChunkData(8080, cmdAttr, dest)
		if err != nil {
			log.Printf("download chunk error:%v\n", err)
			return err
		}
	}

	return nil
}

func DownloadFile(src string, srcFileSize int64, dest string, chunkSize int64) error {
	srcMeta, err := GetRemoteChunksMeta(src, chunkSize)
	if err != nil {
		return err
	}

	destMeta, err := pfsmodules.GetChunksMeta(dest, chunkSize)
	//log.Printf("GetChunkMeta %v\n", dest, err)
	if err != nil {
		if os.IsNotExist(err) {
			if err := pfsmodules.CreateSizedFile(dest, srcFileSize); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	diffMeta, err := pfsmodules.GetDiffChunksMeta(srcMeta, destMeta)
	if err != nil {
		return err
	}

	err = DownloadChunks(src, dest, diffMeta)
	if err != nil {
		return err
	}

	return nil
}

func UploadChunks(src string, dest string, diffMeta []pfsmodules.ChunkMeta) error {
	if len(diffMeta) == 0 {
		log.Printf("srcfile:%s and destfile:%s are same\n", src, dest)
		return nil
	}

	s := NewCmdSubmitter(UserHomeDir() + "/.paddle/config")

	for _, meta := range diffMeta {
		log.Printf("diffMeta:%v\n", meta)
		cmdAttr := pfsmodules.FromArgs("postchunkdata", dest, meta.Offset, meta.Len)
		err := s.PostChunkData(8080, cmdAttr, src)
		if err != nil {
			log.Printf("upload chunk error:%v\n", err)
			return err
		}
	}

	return nil
}

func RemoteTouch(path string, fileSize int64) error {
	resp := pfsmodules.TouchCmdResponse{}
	s := NewCmdSubmitter(UserHomeDir() + "/.paddle/config")

	cmdAttr := pfsmodules.NewTouchCmdAttr(path, fileSize)
	cmd := pfsmodules.NewTouchCmd(cmdAttr, &resp)

	_, err := s.SubmitCmdReqeust("POST", "api/v1/files", 8080, cmd)
	if err != nil {
		log.Printf("error: %v\n", err)
		return err
	}

	if len(resp.Err) > 0 {
		return errors.New(resp.GetErr())
	}

	for _, result := range resp.Results {
		if len(result.Err) > 1 {
			return errors.New(resp.Err)
		}
	}

	log.Printf("touch %s\n", cmdAttr.Args[0])
	return err
}

func UploadFile(src, dest string, srcFileSize int64) error {
	if err := RemoteTouch(dest, srcFileSize); err != nil {
		return err
	}

	dstMeta, err := GetRemoteChunksMeta(dest, pfsmodules.DefaultChunkSize)
	if err != nil {
		return err
	}
	log.Printf("dest %s chunkMeta:%v\n", dest, dstMeta)

	log.Printf("src:%s dest:%s\n", src, dest)
	srcMeta, err := pfsmodules.GetChunksMeta(src, pfsmodules.DefaultChunkSize)
	if err != nil {
		return err
	}
	log.Printf("src %s chunkMeta:%v\n", src, srcMeta)

	diffMeta, err := pfsmodules.GetDiffChunksMeta(srcMeta, dstMeta)
	if err != nil {
		return err
	}

	err = UploadChunks(src, dest, diffMeta)
	if err != nil {
		return err
	}

	return nil
}

func GetRemoteMeta(path string) (*pfsmodules.FileMeta, error) {
	cmdAttr := pfsmodules.NewLsCmdAttr(path, false)
	lsResp, err := RemoteLs(cmdAttr)
	if err != nil {
		return nil, err
	}

	if len(lsResp.Err) > 0 {
		return nil, errors.New(lsResp.Err)
	}

	for _, result := range lsResp.Results {
		if len(result.Err) > 0 {
			return nil, errors.New(lsResp.Err)
		}
		for _, meta := range result.Metas {
			if meta.Path == path {
				return &meta, nil
			}
		}
	}

	return nil, errors.New("internal error")
}

func localLs(path string) (pfsmodules.LsCmdResponse, error) {
	cmdAttr := pfsmodules.NewLsCmdAttr(path, true)
	resp := pfsmodules.LsCmdResponse{}

	lsCmd := pfsmodules.NewLsCmd(cmdAttr, &resp)
	lsCmd.Run()
	if len(resp.Err) > 0 {
		log.Printf("%s error:%s\n", path, resp.GetErr())
		return resp, errors.New(resp.Err)
	}
	return resp, nil
}

func Upload(src, dest string) ([]pfsmodules.CpCmdResult, error) {
	resp, err := localLs(src)
	if err != nil {
		return nil, err
	}

	log.Printf("dest file:%s\n", dest)
	destMeta, err := GetRemoteMeta(dest)
	if err != nil {
		return nil, err
	}

	results := make([]pfsmodules.CpCmdResult, 0, 100)

	for _, result := range resp.Results {
		m := pfsmodules.CpCmdResult{}
		m.Src = result.Path
		_, file := filepath.Split(m.Src)
		if destMeta.IsDir {
			m.Dest = dest + "/" + file
		} else {
			m.Dest = dest
		}

		if len(result.Err) > 0 {
			results = append(results, m)
			log.Printf("%s is a directory\n", m.Src)
			return results, errors.New(result.Err)
		}

		for _, meta := range result.Metas {
			m.Src = meta.Path
			_, file := filepath.Split(meta.Path)
			if destMeta.IsDir {
				m.Dest = dest + "/" + file
			} else {
				m.Dest = dest
			}

			if meta.IsDir {
				m.Err = pfsmodules.OnlySupportUploadOrDownloadFiles
				results = append(results, m)
				log.Printf("%s is a directory\n", meta.Path)
				return results, errors.New(m.Err)
			}

			log.Printf("src_path:%s dest_path:%s\n", m.Src, m.Dest)
			if err := UploadFile(m.Src, m.Dest, meta.Size); err != nil {
				m.Err = err.Error()
				results = append(results, m)
				log.Printf("upload %s  error:%s\n", meta.Path, m.Err)
				return results, errors.New(m.Err)
			}

			results = append(results, m)
		}
	}

	return nil, nil
}

func Download(src, dest string) ([]pfsmodules.CpCmdResult, error) {
	cmdAttr := pfsmodules.NewLsCmdAttr(src, true)

	lsResp, err := RemoteLs(cmdAttr)
	if err != nil {
		return nil, err
	}

	if len(lsResp.Err) > 0 {
		fmt.Printf("%s error:%s\n", src, lsResp.Err)
		return nil, errors.New(lsResp.Err)
	}

	if len(lsResp.Results) > 1 {
		fi, err := os.Stat(dest)
		if err != nil {
			if err == os.ErrNotExist {
				os.MkdirAll(dest, 0755)
			} else {
				return nil, err
			}
		}

		if !fi.IsDir() {
			return nil, errors.New(pfsmodules.DestShouldBeDirectory)
		}
	}

	results := make([]pfsmodules.CpCmdResult, 0, 100)
	m := pfsmodules.CpCmdResult{}
	m.Src = src
	m.Dest = dest

	for _, lsResult := range lsResp.Results {
		for _, meta := range lsResult.Metas {
			m := pfsmodules.CpCmdResult{}
			m.Src = meta.Path
			_, file := filepath.Split(meta.Path)
			m.Dest = dest + "/" + file

			if meta.IsDir {
				m.Err = pfsmodules.OnlySupportUploadOrDownloadFiles
				results = append(results, m)
				log.Printf("%s is a directory\n", meta.Path)
				return results, err
			}

			log.Printf("src_path:%s dest_path:%s\n", m.Src, m.Dest)
			if err := DownloadFile(m.Src, meta.Size, m.Dest, pfsmodules.DefaultChunkSize); err != nil {
				//fmt.Printf("%s error:%s\n", result.Path, result.Err)
				m.Err = err.Error()
				results = append(results, m)
				log.Printf("download %s  error:%s\n", meta.Path, m.Err)
				return results, err
			}

			results = append(results, m)
		}
	}

	return results, nil
}

func RemoteCp(cpCmdAttr *pfsmodules.CpCmdAttr, src, dest string) ([]pfsmodules.CpCmdResult, error) {
	resp := pfsmodules.RemoteCpCmdResponse{}
	cmdAttr := cpCmdAttr.GetNewCmdAttr()
	s := NewCmdSubmitter(UserHomeDir() + "/.paddle/config")

	remoteCpCmd := pfsmodules.NewRemoteCpCmd(cmdAttr, &resp)
	_, err := s.SubmitCmdReqeust("POST", "/api/v1/files", 8080, remoteCpCmd)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return nil, err
	}

	return resp.Results, nil
}
