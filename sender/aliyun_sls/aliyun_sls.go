package aliyun_sls

import (
	"fmt"

	"time"

	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

var _ sender.SkipDeepCopySender = &Sender{}

type Sender struct {
	logProject *sls.LogProject
	store      *sls.LogStore
	name       string
}

func init() {
	sender.RegisterConstructor("aliyun_sls", NewSender)
}

func NewSender(c conf.MapConf) (sls sender.Sender, err error) {
	endpoint, err := c.GetString("sls_endpoint")
	if err != nil {
		return
	}
	project, err := c.GetString("sls_project")
	if err != nil {
		return
	}
	accessKeyId, err := c.GetString("sls_ak")
	if err != nil {
		return
	}
	accessKeySecret, err := c.GetString("sls_sk")
	if err != nil {
		return
	}
	logstore, err := c.GetString("sls_store")
	if err != nil {
		return
	}
	runnerName, _ := c.GetStringOr(sender.KeyName, fmt.Sprintf("sls://%s/%s/%s", endpoint, project, logstore))
	return newSender(runnerName, logstore, project, endpoint, accessKeyId, accessKeySecret)
}

func newSender(runnerName, LogStore, ProjectName, Endpoint, AccessKeyID, AccessKeySecret string) (s *Sender, err error) {
	logProject, _ := sls.NewLogProject(ProjectName, Endpoint, AccessKeyID, AccessKeySecret)
	logStore, err := logProject.GetLogStore(LogStore)
	if err != nil {
		return
	}
	s = &Sender{
		logProject: logProject,
		store:      logStore,
		name:       runnerName,
	}
	return
}

func (s *Sender) Name() string {
	return s.name
}

func (s *Sender) Send(datas []Data) (se error) {
	failure := []Data{}
	var err error
	var lastErr error
	ss := &StatsError{}
	for _, d := range datas {
		var topic string
		var source string
		logs := []*sls.Log{}
		content := []*sls.LogContent{}
		for k, v := range d {
			if k == "__source__" {
				if str, ok := v.(string); ok {
					source = str
					continue
				}
			}
			if k == "__topic__" {
				if str, ok := v.(string); ok {
					topic = str
					continue
				}
			}
			content = append(content, &sls.LogContent{
				Key:   proto.String(fmt.Sprintf("%v", k)),
				Value: proto.String(fmt.Sprintf("%v", v)),
			})
		}
		log := &sls.Log{
			Time:     proto.Uint32(uint32(time.Now().Unix())),
			Contents: content,
		}
		logs = append(logs, log)

		loggroup := &sls.LogGroup{
			Topic:  &topic,
			Source: &source,
			Logs:   logs,
		}

		err = s.store.PutLogs(loggroup)
		if err != nil {
			ss.AddErrors()
			lastErr = err
			failure = append(failure, d)
		} else {
			ss.AddSuccess()
		}
	}
	if len(failure) > 0 && lastErr != nil {
		ss.ErrorDetail = reqerr.NewSendError("Write failure, last err is: "+lastErr.Error(), sender.ConvertDatasBack(failure), reqerr.TypeDefault)
	}
	return ss
}
func (s *Sender) Close() error {
	return nil
}

func (_ *Sender) SkipDeepCopy() bool { return true }
