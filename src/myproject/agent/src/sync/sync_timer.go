package sync

import (
	"../protocal"
	"time"
)

// 定时器发送失败的任务
func (sync *SyncMgr) TimerSendFailedTask() {
	sync.Logger.Infof("start TimerSendFailedTask")
	for {
		select {
		//定时任务取失败文件内容，重发到上传机
		case <-time.After(time.Second * time.Duration(sync.Interval)):
			sync.SendFailedTasks()
		}
	}
}

func (sync *SyncMgr) SendFailedTasks() {
	//获取所有失败的任务
	IdMap := sync.pBin.GetLastOkIdFileMap()
	for k, _ := range IdMap {
		ret, data := sync.pSql.SelectFailedInfo(k)
		if ret != 0 {
			sync.Logger.Errorf("SelectFailedInfo failed ret: %+v, table: %+v",
				ret, k)
			return
		}

		for _, item := range data {
			msg := &protocal.MsgMysqlBody{
				TableName: k,
				Data:      item,
			}

			sync.Logger.Infof("send task in table: %+v, taskid: %+v",
				item.TaskId, k)

			// 迁移数据到备份集群, 失败任务不会重复统计
			if sync.SyncData(msg, false) == 0 {
				// 迁移成功，删除失败任务表中的数据
				if sync.pSql.DeleteFailedTask(item.TaskId, k) != 0 {
					sync.Logger.Errorf("DeleteFailedTask failed taskid: %+v, table: %+v",
						item.TaskId, k)
				}
			}
		}
	}

	return
}
