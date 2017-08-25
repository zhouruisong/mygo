package sync

import (
	"../binlogmgr"
	"../esmgr"
	"../indexmgr"
	"../mysqlmgr"
	"../protocal"
	"../transfer"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
	//	"math/rand"
	//	"strconv"
	"strings"
	//	"time"
)

var openSync = 0
var restartFlag = 0

type SyncMgr struct {
	pBin         *binlogmgr.BinLogMgr
	pTran        *transfer.TransferMgr
	pSql         *mysqlmgr.MysqlMgr
	pEs          *esmgr.EsMgr
	Logger       *log.Logger
	Interval     int
	SyncPartTime bool
	UploadServer string
}

func NewSyncMgr(inval int, server string, my *binlogmgr.BinLogMgr, tran *transfer.TransferMgr,
	sql *mysqlmgr.MysqlMgr, es *esmgr.EsMgr, lg *log.Logger) *SyncMgr {
	sy := &SyncMgr{
		pBin:         my,
		pTran:        tran,
		pSql:         sql,
		pEs:          es,
		Logger:       lg,
		Interval:     inval,
		SyncPartTime: my.GetSyncPartTime(),
		UploadServer: server,
	}

	if sy.InitCache() != nil {
		return nil
	}

	sy.Logger.Infof("NewSyncMgr ok")
	return sy
}

func (sync *SyncMgr) RunDateMigrate() {
	sync.Logger.Infof("start IncreaseSync.")

	// 程序重启时，需要从db获取一段时间内积累的数据同步到备份集群
	if restartFlag == 1 {
		go sync.SendOldSyncTask()
	}

	// 程序启动时，启动定时器处理失败的任务
	go sync.TimerSendFailedTask()

	// 读取上传机发送过来的db数据
	go sync.readIncreaseInfo()

	return
}

func (sync *SyncMgr) HanlePipeCache() {

}

func (sync *SyncMgr) SetFlag(flag, reflag int) {
	openSync = flag
	restartFlag = reflag
}

func (sync *SyncMgr) SendUploadServer(msg *protocal.UploadInfo) (int, error) {
	fmtinfo := "http://%v/index.php?Action=LiveMaintain.FileTaskAddNew&taskid=%s" +
		"&domain=%s&behavior=%s&fname=%s&ftype=%s&url=%s&cb_url=%s&md5_type=%d"
	url := fmt.Sprintf(fmtinfo,
		sync.UploadServer, msg.TaskId, msg.Domain, msg.Behavior, msg.FileName,
		msg.FileType, msg.Url, msg.CbUrl, msg.Md5Type)

	ip := strings.Split(sync.UploadServer, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	sync.Logger.Infof("url: %+v", url)

	body := bytes.NewBuffer([]byte(""))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		sync.Logger.Errorf("http post return failed.err:%v", err)
		return -1, err
	}

	defer res.Body.Close()

	result, err := ioutil.ReadAll(res.Body)
	if err != nil {
		sync.Logger.Errorf("ioutil readall failed, err:%v", err)
		return -1, err
	}

	var ret protocal.RetUploadMeg
	err = json.Unmarshal(result, &ret)
	if err != nil {
		sync.Logger.Errorf("Unmarshal return body error, err:%v", err)
		return -1, err
	}

	sync.Logger.Infof("ret: %+v", ret)

	// 成功
	if ret.Code == 200 {
		return 0, nil
	}

	// 任务已经存在，删除失败表中的该任务，避免重复上传
	if ret.Code == 2 {
		return 2, nil
	}

	return -1, nil
}

func (sync *SyncMgr) sendFileToBackupFdfs(indexcache *protocal.IndexCache, data *protocal.MsgMysqlBody) int {
	// 循环索引记录，分别对每一天记录进行从tair获取id，通过id从fdfs获取文件内容，然后发送到同步服务器
	length := len(indexcache.Item)

	var oldindex string
	filesize1 := fmt.Sprintf("%s\n", indexcache.FileSize)
	oldindex = oldindex + filesize1
	for i := 0; i < length; i++ {
		line := fmt.Sprintf("%s %s %s %s %s\n", indexcache.Item[i].Name, indexcache.Item[i].Id,
			indexcache.Item[i].Status, indexcache.Item[i].Size, indexcache.Item[i].Md5)
		oldindex = oldindex + line
	}
	//sync.Logger.Infof("old index:\n%+v", oldindex)

	for i := 0; i < length; i++ {
		// 根据二级索引中的id，从本集群获取对应的内容
		r, buf := sync.getFileFromFdfs(indexcache.Item[i].Id)
		if r == 0 {
			// 将二级索引中每一片内容上传到备份的fdfs中
			id := sync.pTran.Sendbuff(buf, data.Data.TaskId)
			if id != "" {
				// 将返回的id存储到备份集群的tair中
				//				ret := sync.putIndexFile(data, id)
				//				if ret != 0 {
				//					// 存储到备份集群的tair失败，删除备份集群中该id对于的buff
				//					rt := sync.pTran.Deletebuff(id)
				//					if rt != 0 {
				//						sync.Logger.Errorf("delete data from standby fdfs failed. taskid:%+v, id:%+v",
				//							data.Data.TaskId, id)
				//						return -1
				//					}
				//				}
				// 更新二级索引中的id，换成备份集群的id
				indexcache.Item[i].Id = id
			} else {
				sync.Logger.Errorf("put data to standby fdfs failed. taskid: %+v",
					data.Data.TaskId)
				return -1
			}
		} else {
			sync.Logger.Errorf(" get data to master fdfs failed. taskId: %+v", data.Data.TaskId)
			return -1
		}
	}

	sync.Logger.Infof("send slice buff data to standby fdfs successful, taskid: %+v", data.Data.TaskId)

	//二级索引的所有文件已经转移完毕，请将二级索引文件上传到备份集群的fastdfs并且存储id到备份集群的tair
	var newindex string
	filesize := fmt.Sprintf("%s\n", indexcache.FileSize)
	newindex = newindex + filesize
	for i := 0; i < length; i++ {
		line := fmt.Sprintf("%s %s %s %s %s\n", indexcache.Item[i].Name, indexcache.Item[i].Id,
			indexcache.Item[i].Status, indexcache.Item[i].Size, indexcache.Item[i].Md5)
		newindex = newindex + line
	}
	//sync.Logger.Infof("new index:\n%+v", newindex)

	// 存储新的二级索引内容到备份集群的fdfs中
	buf := []byte(newindex)
	id := sync.pTran.Sendbuff(buf, data.Data.TaskId)
	if id == "" {
		// 二级索引内容存储失败，删除备份集群里该二级索引中每个id片对应的内容
		sync.Logger.Errorf("send index data to standby fdfs failed, taskid: %+v", data.Data.TaskId)
		for i := 0; i < length; i++ {
			// 存储到备份集群的tair失败，删除备份集群中id对于的buff
			rt := sync.pTran.Deletebuff(indexcache.Item[i].Id)
			if rt != 0 {
				sync.Logger.Errorf("delete data from standby fdfs failed, taskid: %+v, id: %+v",
					data.Data.TaskId, indexcache.Item[i].Id)
				return -1
			}
		}
		return -1
	}

	sync.Logger.Infof("send index data to standby fdfs successful, taskid: %+v",
		data.Data.TaskId)

	// 存储新的二级索引的id到备份集群的tair中
	ret := sync.putIndexFile(data, id)
	if ret != 0 {
		sync.Logger.Errorf("send index id to standby tair failed, taskid: %+v, id: %+v",
			data.Data.TaskId, id)
		// 二级索引id存储失败，删除备份集群里该二级索引中每个id片对应的内容
		for i := 0; i < length; i++ {
			// 存储到备份集群的tair失败，删除备份集群中id对于的buff
			rt := sync.pTran.Deletebuff(indexcache.Item[i].Id)
			if rt != 0 {
				sync.Logger.Errorf("delete data from standby fdfs failed, taskid: %+v, id: %+v",
					data.Data.TaskId, indexcache.Item[i].Id)
				return -1
			}
		}

		// 二级索引存储失败，删除备份集群里该二级索引的id对应的内容
		rt := sync.pTran.Deletebuff(id)
		if rt != 0 {
			sync.Logger.Errorf("delete data from standby fdfs failed, taskid: %+v, id :%+v",
				data.Data.TaskId, id)
		}
		return -1
	}

	sync.Logger.Infof("send index id to standby tair successful, taskid: %+v", data.Data.TaskId)

	// 向备份集群的mysql同步数据
	rt := sync.pSql.SendMysqlbuff(&data.Data, data.TableName)
	if rt != 0 {
		sync.Logger.Errorf("send db data to standby mysql failed, taskid: %+v", data.Data.TaskId)
		return -1
	}

	sync.Logger.Infof("send db data to standby mysql successful, taskid: %+v", data.Data.TaskId)
	sync.Logger.Infof("sync data successful, taskid: %+v", data.Data.TaskId)

	return 0
}

func (sync *SyncMgr) getFileFromFdfs(id string) (int, []byte) {
	var ret []byte
	// get file from fdfs
	rt, fileBuff := sync.pTran.PFdfs.HandlerDownloadFile(id)
	if rt == -1 || len(fileBuff) == 0 {
		return -1, ret
	}

	return 0, fileBuff
}

func (sync *SyncMgr) putIndexFile(data *protocal.MsgMysqlBody, id string) int {
	//	creat_time, _ := time.Parse("2006-01-02 15:04:05", data.DbData.CreateTime)
	//	creat_time_u := creat_time.Unix()
	//
	//	expiry_time, _ := time.Parse("2006-01-02 15:04:05", data.DbData.ExpiryTime)
	//	expiry_time_u := expiry_time.Unix()

	keys := protocal.SendTairPut{
		Prefix:     data.Data.Domain,
		Key:        "/" + data.Data.Domain + data.Data.Uri + ".index",
		Value:      id,
		CreateTime: data.Data.CreateTime,
		ExpireTime: data.Data.ExpiryTime,
	}

	var msg protocal.SednTairPutBody
	msg.Keys = append(msg.Keys, keys)
	return sync.pTran.PTair.SendToBackUpTair(&msg)
}

func (sync *SyncMgr) getIndexFile(prefix string, key string) *protocal.RetTairGet {
	keys := protocal.SendTairGet{
		Prefix: prefix,
		Key:    "/" + prefix + key + ".index",
	}

	var msg protocal.SednTairGetBody
	msg.Keys = append(msg.Keys, keys)

	buf, err := json.Marshal(msg)
	if err != nil {
		sync.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return nil
	}

	var ret protocal.RetTairGet
	ret.Errno, ret.Keys = sync.pTran.PTair.HandlerSendtoTairGet(buf)

	return &ret
}

func (sync *SyncMgr) getIndexFileFromFdfs(pData *protocal.DbInfo) (int, []byte) {
	// first get index id from tair
	var ret_byte []byte
	ret := sync.getIndexFile(pData.Domain, pData.Uri)
	if ret.Errno != 0 {
		sync.Logger.Errorf("ret: %+v", ret)
		return -1, ret_byte
	}

	// get index file from fdfs
	rt, fileBuff := sync.pTran.PFdfs.HandlerDownloadFile(ret.Keys[0].Value)
	if rt == -1 || len(fileBuff) == 0 {
		return -1, ret_byte
	}

	return 0, fileBuff
}

func (sync *SyncMgr) sendToEs(pData *protocal.DbInfo) int {
	tmp_time := strings.Split(pData.CreateTime, " ")

	if len(tmp_time) < 2 {
		sync.Logger.Infof("pData:%+v", pData)
		return -1
	}
	creat_time := fmt.Sprintf("%s%s%s%s", tmp_time[0], "T", tmp_time[1], "+08:00")

	var action int32
	if pData.SourceType == "DEL" {
		action = 1
	} else {
		action = 0
	}

	msg := &protocal.SendEsBody{
		TaskId:     pData.TaskId,
		Action:     action,
		Domain:     pData.Domain,
		FileName:   pData.FileName,
		Filesize:   pData.FileSize,
		Uri:        pData.Uri,
		CreateTime: creat_time,
	}

	return sync.pEs.HandlerSendToEs(msg)
}

func (sync *SyncMgr) getFromEs(pData *protocal.DbInfo) int {
	msg := &protocal.GetEsInput{
		Domain:   pData.Domain,
		FileName: pData.FileName,
	}
	// 获取指定用户的文件
	return sync.pEs.HandlerGetFromEs(msg)
}

// 保存最后收到的task信息
func (sync *SyncMgr) SaveLastSuccessIdToFile(id int, tablename string) int {
	return sync.pBin.WriteTaskIdTofile(id, tablename)
}

func (sync *SyncMgr) InsertFailedTask(data *protocal.MsgMysqlBody) int {
	return sync.pSql.InsertFailedTask(&data.Data, data.TableName)
}

func (sync *SyncMgr) DeleteFailedTask(data *protocal.MsgMysqlBody) int {
	return sync.pSql.DeleteFailedTask(data.Data.TaskId, data.TableName)
}

func (sync *SyncMgr) NonUploadMachineSync(data *protocal.MsgMysqlBody, insertdb bool) int {
	result := 0
	// 获取索引
	ret, buff := sync.getIndexFileFromFdfs(&data.Data)
	if ret != 0 {
		sync.Logger.Errorf("getIndexFileFromFdfs failed, domain:%+v, prefix:+%v",
			data.Data.Domain, data.Data.Uri)
		result = -1
	} else {
		// 解析索引
		var indexcache protocal.IndexCache
		err := indexmgr.ReadLine(buff, &indexcache)
		//sync.Logger.Infof("indexcache: %+v", indexcache)
		if err != nil {
			sync.Logger.Errorf("ReadLine err:%+v,taskid: %+v", err, data.Data.TaskId)
			result = -1
		}

		// 发送二级索引中每个id片对应的内容到备份集群
		if sync.sendFileToBackupFdfs(&indexcache, data) != 0 {
			sync.Logger.Errorf("sendFileToBackupFdfs failed,taskid: %+v", data.Data.TaskId)
			result = -1
		}
	}

	if result != 0 {
		// 记录上传失败的内容，有定时线程从数据库获取二级索引文件，重新上传
		if insertdb && sync.InsertFailedTask(data) != 0 {
			sync.Logger.Errorf("InsertFailedInfo failed tablename: %+v, taskid: %+v",
				data.TableName, data.Data.TaskId)
		}
	}

	return result
}

func (sync *SyncMgr) UploadMachineSync(data *protocal.MsgMysqlBody, insertdb bool) int {
	msg := &protocal.UploadInfo{
		TaskId:   data.Data.TaskId,
		Domain:   data.Data.Domain,
		FileName: data.Data.FileName,
		FileType: data.Data.FileType,
		Url:      data.Data.Uri,
		CbUrl:    data.Data.CbUrl,
		Behavior: "UP",
		Md5Type:  0,
		IsBackup: data.Data.IsBackup,
	}

	// 发送到上传机
	ret, _ := sync.SendUploadServer(msg)
	if ret == -1 {
		sync.Logger.Errorf("SendUploadServer failed")
		// 记录上传失败的内容，有定时线程反复上传该数据
		if insertdb && sync.InsertFailedTask(data) != 0 {
			sync.Logger.Errorf("InsertFailedInfo failed taskid:%+v", data.Data.TaskId)
		}
		return -1
	}

	// 任务已经存在，删除失败表中的记录
	if ret == 2 {
		if sync.DeleteFailedTask(data) != 0 {
			sync.Logger.Errorf("DeleteFailedTask failed taskid:%+v", data.Data.TaskId)
		}
	}

	return 0
}

func (sync *SyncMgr) SyncData(data *protocal.MsgMysqlBody, insertdb bool) int {
	if openSync == 0 {
		// 给上传机发送请求数据迁移
		return sync.UploadMachineSync(data, insertdb)
	} else {
		// 本服务负责数据迁移
		return sync.NonUploadMachineSync(data, insertdb)
	}
}

// read binlog data from pipe
func (sync *SyncMgr) readIncreaseInfo() {
	sync.Logger.Infof("start readIncreaseInfo.")
	for {
		data := sync.pBin.Read()
		if data == nil {
			break
		}
		// 迁移数据到备份集群
		sync.SyncData(data, true)
		// 发送结构文件到es，用于统计数据使用, 对于失败的任务不会在统计
		ret := sync.sendToEs(&data.Data)
		if ret != 0 {
			sync.Logger.Errorf("sendToEs failed taskid:%+v", data.Data.TaskId)
		}
	}

	sync.Logger.Infof("stop readIncreaseInfo.")
	return
}

/*
func (sync *SyncMgr) InsertToDb() {
	// test insert
	time.Sleep(10 * time.Second)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	//10以内的随机数
	num := int(r.Int31n(1000))

	sync.Logger.Infof("start insert.")
	for i := 0; i < num; i++ {
		var filesize int32

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		//10以内的随机数
		filesize = int32(r.Int31n(100))

		taskid := "zhouruisong" + strconv.Itoa(i)
		filename := taskid + ".mp4"
		Md5 := fmt.Sprintf("%s%s", "0fb2eae9de4c1d4", taskid)
		url := fmt.Sprintf("%s/%s", "http://twin14602.sandai.net/tools/coral", filename)

		time.Sleep(1 * time.Second)

		a := time.Now()
		creat_time := fmt.Sprintf("%s", a.Format("2006-01-02 15:04:05"))

		info := &protocal.DbInfo{
			TaskId:     taskid,
			FileName:   filename,
			FileType:   "mp4",
			FileSize:   filesize,
			Domain:     "www.wasu.cn",
			Status:     200,
			Action:     "UP",
			Md5Type:    1,
			CreateTime: creat_time,
			DnameMd5:   Md5,
			SourceUrl:  url,
			FileMd5:    Md5,
			CbUrl:      url,
			FfUri:      url,
			Type:       0,
		}
		sync.pSql.InsertFailedTask(info, "t_livefcup")
	}

	sync.Logger.Infof("insert end.")
}*/
