package paddlecloud

import (
	"context"
	"flag"
	"fmt"
	pfsmod "github.com/PaddlePaddle/cloud/go/filemanager/pfsmodules"
	"github.com/google/subcommands"
)

type LsCommand struct {
	r bool
}

func (*LsCommand) Name() string     { return "ls" }
func (*LsCommand) Synopsis() string { return "List files on PaddlePaddle Cloud" }
func (*LsCommand) Usage() string {
	return `ls [-r] <pfspath>:
	List files on PaddlePaddleCloud
	Options:
`
}

func (p *LsCommand) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&p.r, "r", false, "list files recursively")
}

//func RemoteLs(path string, r bool) (*pfsmod.LsCmdResponse, error) {
func RemoteLs(cmdAttr *pfsmod.CmdAttr) (*pfsmod.LsCmdResponse, error) {
	resp := pfsmod.LsCmdResponse{}
	s := NewCmdSubmitter(UserHomeDir() + "/.paddle/config")

	lsCmd := pfsmod.NewLsCmd(cmdAttr, &resp)
	_, err := s.SubmitCmdReqeust("GET", "api/v1/files", 8080, lsCmd)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return &resp, err
	}

	return &resp, err

}

func (p *LsCommand) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if f.NArg() < 1 {
		f.Usage()
		return subcommands.ExitFailure
	}

	cmdAttr := pfsmod.NewCmdAttr(p.Name(), f)

	resp, err := RemoteLs(cmdAttr)
	if err != nil {
		return subcommands.ExitFailure
	}

	fmt.Println(resp)

	return subcommands.ExitSuccess
}
