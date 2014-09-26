package nsqd

type context struct {
	nsqd *NSQD
	l    logger
}

// 向TCP服务器添加自定义的logger