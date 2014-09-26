package nsqd

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/bitly/nsq/util"
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64

	sync.RWMutex

	name              string
	channelMap        map[string]*Channel
	backend           BackendQueue
	incomingMsgChan   chan *Message
	memoryMsgChan     chan *Message
	exitChan          chan int
	channelUpdateChan chan int
	waitGroup         util.WaitGroupWrapper
	exitFlag          int32

	paused    int32
	pauseChan chan bool

	ctx *context
}

// Topic constructor
func NewTopic(topicName string, ctx *context) *Topic {
	diskQueue := newDiskQueue(topicName,
		ctx.nsqd.opts.DataPath,
		ctx.nsqd.opts.MaxBytesPerFile,
		ctx.nsqd.opts.SyncEvery,
		ctx.nsqd.opts.SyncTimeout,
		ctx.l)

	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		backend:           diskQueue,
		incomingMsgChan:   make(chan *Message, 1),
		memoryMsgChan:     make(chan *Message, ctx.nsqd.opts.MemQueueSize),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		ctx:               ctx,
		pauseChan:         make(chan bool),
	}

// router将put来的消息放到合适的队列
	t.waitGroup.Wrap(func() { t.router() })
// 消息处理
	t.waitGroup.Wrap(func() { t.messagePump() })
// 启动时持久化nsqd元数据
	go t.ctx.nsqd.Notify(t)

	return t
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// 线程安全供外部使用的GetChannel，会新建不存在的channel

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		
//这里为什么还要加select的方式向管道写入数据？答：无缓冲管道有内容没有读取时阻塞，为了能够及时退出
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}
// 内部函数，非线程安全
// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		// channel有删除回调，在channel确定自身已经删除完成时从topic中删除entry
		channel = NewChannel(t.name, channelName, t.ctx, deleteCallback)
		t.channelMap[channelName] = channel
		t.ctx.l.Output(2, fmt.Sprintf("TOPIC(%s): new channel(%s)", t.name, channel.name))
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// 不存在也不会创建
// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	t.Unlock()

	t.ctx.l.Output(2, fmt.Sprintf("TOPIC(%s): deleting channel %s", t.name, channel.name))

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

// PutMessage 写入incoming管道即完成
// PutMessage writes to the appropriate incoming message channel
func (t *Topic) PutMessage(msg *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	if !t.ctx.nsqd.IsHealthy() {
		return errors.New("unhealthy")
	}
	t.incomingMsgChan <- msg
	atomic.AddUint64(&t.messageCount, 1)
	return nil
}

func (t *Topic) PutMessages(messages []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	if !t.ctx.nsqd.IsHealthy() {
		return errors.New("unhealthy")
	}
	for _, m := range messages {
		t.incomingMsgChan <- m
		atomic.AddUint64(&t.messageCount, 1)
	}
	return nil
}

// 内存+磁盘队列总深度
func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan chan []byte

	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()

// 不直接用topic结构中的管道参与循环的原因：可以通过将循环中的管道置为nil的方式来pause
	if len(chans) > 0 {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

// 无限循环处理memory和disk上的消息
	for {
		select {
		case msg = <-memoryMsgChan:
			// 内存队列有数据，直接读到msg
		case buf = <-backendChan:
			// 内存处理完成，开始处理磁盘队列，反序列化成msg格式
			msg, err = decodeMessage(buf)
			if err != nil {
				t.ctx.l.Output(2, fmt.Sprintf("ERROR: failed to decode message - %s", err))
				continue
			}
		case <-t.channelUpdateChan:
			// channel有增减，先清空循环中用到的chan数组，再重新从topic的map中加载。
			chans = make([]*Channel, 0)
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			// 手动pause topic或者没有channel处理时，pause topic，继续接收消息但不处理
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case pause := <-t.pauseChan:
			// pause 的实现：循环变量中的chan置为nil；unpause:置为topic中的chan
			if pause || len(chans) == 0 {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			goto exit
		}

// 对所有channel发送msg，如果不止一个chan处理该msg，则在此复制。
		for i, channel := range chans {
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
			}
			err := channel.PutMessage(chanMsg)
			if err != nil {
				t.ctx.l.Output(2, fmt.Sprintf(
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err))
			}
		}
	}

exit:
	t.ctx.l.Output(2, fmt.Sprintf("TOPIC(%s): closing ... messagePump", t.name))
}

// 对接收到的消息进行路由，从incomingChan读取后，如memoryChan未满则放入memoryChan否则写入backend，写入backend失败时nsqd的health降为0
// router handles muxing of Topic messages including
// proxying messages to memory or backend
func (t *Topic) router() {
	var msgBuf bytes.Buffer
	for msg := range t.incomingMsgChan {
		select {
		case t.memoryMsgChan <- msg:
		default:
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				t.ctx.l.Output(2, fmt.Sprintf(
					"TOPIC(%s) ERROR: failed to write message to backend - %s",
					t.name, err))
				t.ctx.nsqd.SetHealth(err)
			}
		}
	}

	t.ctx.l.Output(2, fmt.Sprintf("TOPIC(%s): closing ... router", t.name))
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

// topic退出，true为清空消息，false不清空
func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		t.ctx.l.Output(2, fmt.Sprintf("TOPIC(%s): deleting", t.name))

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		go t.ctx.nsqd.Notify(t)
	} else {
		t.ctx.l.Output(2, fmt.Sprintf("TOPIC(%s): closing", t.name))
	}

// 关闭exitChan等于向其发送消息，此后发生的写入会收到“exiting”
	close(t.exitChan)
// 停止接收新数据
	t.Lock()
	close(t.incomingMsgChan)
	t.Unlock()

// 等待消息处理和路由循环结束
	// synchronize the close of router() and messagePump()
	t.waitGroup.Wait()

	if deleted {
		// 对所有channel调用其delete
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		// 清空topic的内存和backend队列
		t.Empty()
		// backend直接删除
		return t.backend.Delete()
	}

// delete=false的时候这里才会执行，否则前面已经返回了。
	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.ctx.l.Output(2, fmt.Sprintf("ERROR: channel(%s) close - %s", channel.name, err))
		}
	}

	// write anything leftover to disk
	t.flush()
	return t.backend.Close()
}

// 先排空内存队列，再调用backend的Empty
func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}

// 将内存队列的数据写入磁盘队列
func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		t.ctx.l.Output(2, fmt.Sprintf(
			"TOPIC(%s): flushing %d memory messages to backend", t.name, len(t.memoryMsgChan)))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				t.ctx.l.Output(2, fmt.Sprintf(
					"ERROR: failed to write message to backend - %s", err))
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *util.Quantile {
	var latencyStream *util.Quantile
	for _, c := range t.channelMap {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = util.NewQuantile(
				t.ctx.nsqd.opts.E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.opts.E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

// 每次pause或者unpause一个topic时，持久化数据
func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- pause:
		t.ctx.nsqd.Lock()
		defer t.ctx.nsqd.Unlock()
		// pro-actively persist metadata so in case of process failure
		// nsqd won't suddenly (un)pause a topic
		return t.ctx.nsqd.PersistMetadata()
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}
