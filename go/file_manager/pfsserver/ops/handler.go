package pfsserver

import (
	//"encoding/json"
	//"github.com/cloud/go/file_manager/pfscommon"
	//"fmt"
	//"github.com/cloud/go/file_manager/pfscommon"
	"github.com/cloud/go/file_manager/pfsmodules"
	"io"
	"log"
	//"mime/multipart"
	//"mime/multipart"
	"net/http"
	//"os"
	"strconv"
)

func lsCmdHandler(w http.ResponseWriter, req *pfsmodules.CmdAttr) {
	resp := pfsmodules.LsCmdResponse{}

	log.Print(req)

	cmd := pfsmodules.NewLsCmd(req, &resp)
	cmd.RunAndResponse(w)

	return
}

func MD5SumCmdHandler(w http.ResponseWriter, req *pfsmodules.CmdAttr) {
	resp := pfsmodules.MD5SumResponse{}
	log.Print(req)

	cmd := pfsmodules.NewMD5SumCmd(req, &resp)
	cmd.RunAndResponse(w)
}

func GetFilesHandler(w http.ResponseWriter, r *http.Request) {
	resp := pfsmodules.LsCmdResponse{}
	req, err := pfsmodules.GetJsonRequestCmdAttr(r)
	if err != nil {
		resp.SetErr(err.Error())
		pfsmodules.WriteCmdJsonResponse(w, &resp, 422)
		return
	}

	if len(req.Args) == 0 {
		resp.SetErr("no args")
		pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusExpectationFailed)
		return

	}

	switch req.Method {
	case "ls":
		lsCmdHandler(w, req)
	case "md5sum":
		MD5SumCmdHandler(w, req)
	default:
		resp.SetErr(http.StatusText(http.StatusMethodNotAllowed))
		pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusMethodNotAllowed)
	}

	log.Print(req)
}

func rmCmdHandler(w http.ResponseWriter, req *pfsmodules.CmdAttr) {
	resp := pfsmodules.RmCmdResponse{}

	log.Print(req)

	cmd := pfsmodules.NewRmCmd(req, &resp)
	cmd.RunAndResponse(w)

	return
}

func touchHandler(w http.ResponseWriter, req *pfsmodules.CmdAttr) {
	resp := pfsmodules.TouchCmdResponse{}

	//log.Print(req)

	cmd := pfsmodules.NewTouchCmd(req, &resp)
	cmd.RunAndResponse(w)

	return
}

func PostFilesHandler(w http.ResponseWriter, r *http.Request) {
	resp := pfsmodules.JsonResponse{}
	req, err := pfsmodules.GetJsonRequestCmdAttr(r)
	if err != nil {
		resp.SetErr(err.Error())
		pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusExpectationFailed)
		return
	}

	if len(req.Args) == 0 {
		resp.SetErr("no args")
		pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusExpectationFailed)
		return

	}

	log.Print(req)

	switch req.Method {
	case "rm":
		rmCmdHandler(w, req)
	case "touch":
		if len(req.Args) != 1 {
			resp.SetErr("please create only one file")
			pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusExpectationFailed)
			return
		}
		touchHandler(w, req)
	default:
		resp.SetErr(http.StatusText(http.StatusMethodNotAllowed))
		pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusMethodNotAllowed)
	}
}

func GetChunksMetaHandler(w http.ResponseWriter, r *http.Request) {
	method := r.URL.Query().Get("method")

	log.Println(r.URL.String())

	resp := pfsmodules.JsonResponse{}
	switch method {
	case "getchunkmeta":
		cmd := pfsmodules.GetChunkMetaCmd(w, r)
		if cmd == nil {
			return
		}
		cmd.RunAndResponse(w)
	default:
		resp.SetErr(http.StatusText(http.StatusMethodNotAllowed))
		pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusMethodNotAllowed)
	}
}

func GetChunksHandler(w http.ResponseWriter, r *http.Request) {
	/*
		resp := pfsmodules.JsonResponse{}
		req, err := pfsmodules.NewChunkCmdAttr(r)
		if err != nil {
			resp.SetErr(err.Error())
			pfsmodules.WriteCmdJsonResponse(w, &resp, 422)
			return
		}
	*/
	log.Printf("GetChunksHandler\n")
	method := r.URL.Query().Get("method")
	log.Println(r.URL.String())
	resp := pfsmodules.JsonResponse{}

	switch method {
	case "getchunkdata":
		req, err := pfsmodules.GetChunkRequest(r)
		if err != nil {
			resp.SetErr(err.Error())
			pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusBadRequest)
			return
		}

		//contentType := fmt.Sprintf("Content-Type: multipart/form-data; boundary=%s",
		//pfscommon.MultiPartBoundary)
		//w.Header().Set("Content-Type", contentType)
		//w.WriteHeader(status)

		if err := pfsmodules.WriteStreamChunkData(req.Path, req.Offset, req.ChunkSize, w); err != nil {
			/*
				resp.SetErr(err.Error())
				pfsmodules.WriteCmdJsonResponse(w, &resp, 422)
			*/
			return
		}
	default:
		resp.SetErr(http.StatusText(http.StatusMethodNotAllowed))
		pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusMethodNotAllowed)
	}

	return
}

func PostChunksHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("PostChunksHandler\n")
	resp := pfsmodules.JsonResponse{}
	partReader, err := r.MultipartReader()

	if err != nil {
		resp.SetErr("error:" + err.Error())
		pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusBadRequest)
		return
	}

	for {
		part, error := partReader.NextPart()
		if error == io.EOF {
			break
		}

		if part.FormName() == "chunk" {
			chunkCmdAttr, err := pfsmodules.ParseFileNameParam(part.FileName())
			if err != nil {
				resp.SetErr("error:" + err.Error())
				pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusInternalServerError)
				break
			}

			f, err := pfsmodules.GetChunkWriter(chunkCmdAttr.Path, chunkCmdAttr.Offset)
			if err != nil {
				resp.SetErr("open " + chunkCmdAttr.Path + "error:" + err.Error())
				pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusInternalServerError)
				//return err
				break
			}
			defer f.Close()

			writen, err := io.Copy(f, part)
			if err != nil || writen != int64(chunkCmdAttr.ChunkSize) {
				resp.SetErr("read " + strconv.FormatInt(writen, 10) + "error:" + err.Error())
				pfsmodules.WriteCmdJsonResponse(w, &resp, http.StatusBadRequest)
				//return err
				break
			}
		}
	}
}
