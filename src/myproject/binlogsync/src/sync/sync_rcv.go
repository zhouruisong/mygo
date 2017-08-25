package sync

import (
	"../protocal"
	"encoding/json"
	//	"fmt"
	"io/ioutil"
	"net/http"
)

// 接收上传机发送的db消息
func (sync *SyncMgr) Login(res http.ResponseWriter, req *http.Request) {
	//	req.ParseForm()
	//	fmt.Println(req)
	sync.Logger.Infof("zhouruisong")
	var ret protocal.RetCentreUploadFile
	ret.Errno = 0
	ret.Errmsg = "success"
	ret.Id = "1"
	b, _ := json.Marshal(ret)
	res.Write(b)
}

// 接收发送的文件消息，存入fastdfs
func (sync *SyncMgr) FastdfsPutData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var id string
	var msg string
	var b []byte
	var err_marshal error
	var ret protocal.RetCentreUploadFile

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil {
		sync.Logger.Errorf("ReadAll failed. err:%v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		sync.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt, id, msg = sync.pTran.HandlerUploadData(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
		ret.Id = id
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		sync.Logger.Errorf("Marshal failed. err:%v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "failed"
		ret.Id = ""
		goto END
	}

	sync.Logger.Infof("return: %+v", ret)
END:
	res.Write(b) // HTTP 200
}

// 接收发送的文件消息，存入fastdfs
func (sync *SyncMgr) FastdfsGetData(res http.ResponseWriter, req *http.Request) {
	var rt int
	var msg string
	var content []byte
	var b []byte
	var err_marshal error
	var ret protocal.RetCentreDownloadFile

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		sync.Logger.Errorf("ReadAll failed. %v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		sync.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt, content, msg = sync.pTran.HandlerDownloadData(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
		ret.Content = content
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		sync.Logger.Errorf("Marshal failed. %v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	sync.Logger.Infof("return: %+v", ret)
END:
	res.Write(b) // HTTP 200
}

// 接收发送的文件消息，存入fastdfs
func (sync *SyncMgr) FastdfsDeleteData(res http.ResponseWriter, req *http.Request) {
	var rt error
	var b []byte
	var msg string
	var err_marshal error
	var ret protocal.RetCentreDeleteFile

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		sync.Logger.Errorf("ReadAll failed. %v", err)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}
	if len(buf) == 0 {
		sync.Logger.Errorf("buf len = 0")
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	rt, msg = sync.pTran.HandlerDeleteData(buf)
	if rt != nil {
		ret.Errno = -1
		ret.Errmsg = msg
	} else {
		ret.Errno = 0
		ret.Errmsg = msg
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		sync.Logger.Errorf("Marshal failed. %v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "failed"
		goto END
	}

	sync.Logger.Infof("return: %+v", ret)
END:
	res.Write(b) // HTTP 200
}

// 接收DB同步过来的内容，插入对应的live_master表中
func (sync *SyncMgr) MysqlReceive(res http.ResponseWriter, req *http.Request) {
	buf, err := ioutil.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		sync.Logger.Errorf("ReadAll failed. %v", err)
	}

	var ret protocal.MsgMysqlRet
	rt, msg := sync.pSql.Handlerdbinsert(buf)

	if rt == 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
	}

	b, err := json.Marshal(ret)
	if err != nil {
		sync.Logger.Errorf("Marshal failed. %v", err)
	}

	sync.Logger.Infof("return: %+v", ret)
	res.Write(b) // HTTP 200
}

// 接收发送过来的上传tair消息，将id存入tair
func (sync *SyncMgr) TairReceive(res http.ResponseWriter, req *http.Request) {
	var rt int
	var b []byte
	var msg string
	var err_marshal error
	var ret protocal.MsgTairRet

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil {
		sync.Logger.Errorf("ReadAll failed err:%v", err)
		ret.Errno = -1
		ret.Errmsg = "Readall failed"
		goto END
	}
	if len(buf) == 0 {
		ret.Errno = -1
		ret.Errmsg = "buf len = 0"
		goto END
	}

	rt, msg = sync.pTran.PTair.HandlerSendtoTairPut(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		sync.Logger.Errorf("Marshal failed err:%v", err_marshal)
		ret.Errno = -1
		ret.Errmsg = "marshal failed"
		goto END
	}

	sync.Logger.Infof("return: %+v", ret)
END:
	res.Write(b) // HTTP 200
}

// 接收上传机发送的db消息
func (sync *SyncMgr) UploadPut(res http.ResponseWriter, req *http.Request) {
	var rt int
	var msg string
	var b []byte
	var err_marshal error
	var ret protocal.MsgMysqlRet

	buf, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil {
		ret.Errno = -1
		ret.Errmsg = "ReadAll failed"
		goto END
	}
	if len(buf) == 0 {
		ret.Errno = -1
		ret.Errmsg = "buf len = 0"
		goto END
	}

	rt, msg = sync.handleUploadData(buf)
	if rt != 0 {
		ret.Errno = rt
		ret.Errmsg = msg
	} else {
		ret.Errno = rt
		ret.Errmsg = msg
	}

	b, err_marshal = json.Marshal(ret)
	if err_marshal != nil {
		ret.Errno = -1
		ret.Errmsg = "Marshal failed"
		goto END
	}

	if ret.Errno != 0 {
		sync.Logger.Infof("return: %+v", ret)
	}

END:
	res.Write(b) // HTTP 200
}

// 处理函数
func (sync *SyncMgr) handleUploadData(buf []byte) (int, string) {
	var data protocal.MsgMysqlBody
	err := json.Unmarshal(buf, &data)
	if err != nil {
		sync.Logger.Errorf("Unmarshal error err:%v", err)
		return -1, "Unmarshal failed"
	}

	//sync.Logger.Infof("data: %+v", data)
	// 更新成功收到的Id，重启服务时可以指定是否大于这个id增量上传
	if sync.SaveLastSuccessIdToFile(data.Data.Id, data.TableName) != 0 {
		sync.Logger.Errorf("SaveLastSuccessIdToFile failed taskid: %+v, tablename: %+v",
			data.Data.TaskId, data.TableName)
		return -1, "SaveLastSuccessIdToFile failed"
	}

	//data.Status == 0 表示是源数据处理完毕，需要同步， 为1表示是备份数据，不需要同步
	if data.Data.IsBackup == 0 {
		// 指定时间同步，需要先写文件
		if sync.SyncPartTime {
			//			sync.SaveFailedTofile(fmt.Sprintf("http://%s/%s/%s", "127.0.0.1",
			//				data.Data.Domain, data.Data.FileName))
		} else {
			if sync.pBin.Write(&data) != 0 {
				sync.Logger.Errorf("write channel failed: taskid: %+v", data.Data.TaskId)
				return -1, "write channel failed"
			}
		}
	} else {
		sync.Logger.Errorf("data.Data.IsBackup = %+v", data.Data.IsBackup)
	}

	sync.Logger.Infof("get upload msg successful, taskid: %+v", data.Data.TaskId)
	return 0, "ok"
}
