package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/pqueue"
)

// the amount of time a worker will wait when idle
const defaultWorkerWait = 100 * time.Millisecond

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64

	sync.RWMutex

	topicName string
	name      string
	ctx       *context

	backend BackendQueue

	incomingMsgChan chan *Message
	memoryMsgChan   chan *Message
	clientMsgChan   chan *Message
	exitChan        chan int
	waitGroup       util.WaitGroupWrapper
	exitFlag        int32

	// state tracking
	clients          map[int64]Consumer
	paused           int32
	ephemeralChannel bool
	deleteCallback   func(*Channel)
	deleter          sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *util.Quantile

	// TODO: these can be DRYd up
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex

	// stat counters
	bufferedCount int32
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:       topicName,
		name:            channelName,
		incomingMsgChan: make(chan *Message, 1),
		memoryMsgChan:   make(chan *Message, ctx.nsqd.opts.MemQueueSize),
		clientMsgChan:   make(chan *Message),
		exitChan:        make(chan int),
		clients:         make(map[int64]Consumer),
		deleteCallback:  deleteCallback,
		ctx:             ctx,
	}
	if len(ctx.nsqd.opts.E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = util.NewQuantile(
			ctx.nsqd.opts.E2EProcessingLatencyWindowTime,
			ctx.nsqd.opts.E2EProcessingLatencyPercentiles,
		)
	}

	c.initPQ()

// #ephemeral结尾的是临时性的channel，backend实际不做任何处理
	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeralChannel = true
		c.backend = newDummyBackendQueue()
	} else {
		// backend names, for uniqueness, automatically include the topic...
		backendName := getBackendName(topicName, channelName)
		c.backend = newDiskQueue(backendName,
			ctx.nsqd.opts.DataPath,
			ctx.nsqd.opts.MaxBytesPerFile,
			ctx.nsqd.opts.SyncEvery,
			ctx.nsqd.opts.SyncTimeout,
			ctx.l)
	}

// 这里的messagePump没有用waitGroup等待，因为随时停止pump不会丢失消息
	go c.messagePump()
// incoming发送到memory或backendQueue
	c.waitGroup.Wrap(func() { c.router() })
	c.waitGroup.Wrap(func() { c.deferredWorker() })
	c.waitGroup.Wrap(func() { c.inFlightWorker() })

	go c.ctx.nsqd.Notify(c)

	return c
}

func (c *Channel) initPQ() {
	// 优先级队列的长度是内存队列长度的十分之一
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.opts.MemQueueSize)/10))

// inFlight是自行实现的小根堆，deferred是heap库实现的小根堆，要用两种不同的实现的原因：
// deferred队列不应该改变原有message的任何东西，因此只能在外附加一层pri和index用来实现优先级排序

// 内存中的message，message结构自身含有优先级
	c.inFlightMessages = make(map[MessageID]*Message)
// 延迟的队列，消息加上pqueue.Item优先级
	c.deferredMessages = make(map[MessageID]*pqueue.Item)

	c.inFlightMutex.Lock()
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		c.ctx.l.Output(2, fmt.Sprintf("CHANNEL(%s): deleting", c.name))

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd

		// Notify和从lookupd解除注册有什么关系？
		go c.ctx.nsqd.Notify(c)
	} else {
		c.ctx.l.Output(2, fmt.Sprintf("CHANNEL(%s): closing", c.name))
	}

	// this forceably closes client connections
	// 关闭所有连接到channel的client
	c.RLock()
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	close(c.exitChan)

	// handle race condition w/ things writing into incomingMsgChan
	c.Lock()
	close(c.incomingMsgChan)
	c.Unlock()

	// synchronize the close of router() and pqWorkers (2)
	c.waitGroup.Wait()

	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}

func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()
	for _, client := range c.clients {
		client.Empty()
	}

// 排空clientChan和memoryChan
	// 从nil管道读取会堵塞。not ok：管道已经关闭
	clientMsgChan := c.clientMsgChan
	for {
		select {
		case _, ok := <-clientMsgChan:
			if !ok {
				// c.clientMsgChan may be closed while in this loop
				// so just remove it from the select so we can make progress
				clientMsgChan = nil
			}
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	// messagePump is responsible for closing the channel it writes to
	// this will read until its closed (exited)
	for msg := range c.clientMsgChan {
		c.ctx.l.Output(2, fmt.Sprintf(
			"CHANNEL(%s): recovered buffered message from clientMsgChan", c.name))

		// 带缓冲的调用backend的Put方法，写入backend
		writeMessageToBackend(&msgBuf, msg, c.backend)
	}

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.l.Output(2, fmt.Sprintf(
			"CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages)))
	}

	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.l.Output(2, fmt.Sprintf(
					"ERROR: failed to write message to backend - %s", err))
			}
		default:
			goto finish
		}
	}

finish:
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.l.Output(2, fmt.Sprintf(
				"ERROR: failed to write message to backend - %s", err))
		}
	}

	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.l.Output(2, fmt.Sprintf(
				"ERROR: failed to write message to backend - %s", err))
		}
	}

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth() + int64(atomic.LoadInt32(&c.bufferedCount))
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()

	c.ctx.nsqd.Lock()
	defer c.ctx.nsqd.Unlock()
	// pro-actively persist metadata so in case of process failure
	// nsqd won't suddenly (un)pause a channel
	return c.ctx.nsqd.PersistMetadata()
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes to the appropriate incoming message channel
// (which will be routed asynchronously)
func (c *Channel) PutMessage(msg *Message) error {
	c.RLock()
	defer c.RUnlock()
	if atomic.LoadInt32(&c.exitFlag) == 1 {
		return errors.New("exiting")
	}
	c.incomingMsgChan <- msg
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

// 下面的Touch, Finish和Requeue都未在channel自身调用，是提供给外部的接口。何时调用要看来自remote的消息。

// 从InFlight取出msg，改变其pri并放回InFlight（字典和PQ）
// TouchMessage resets the timeout for an in-flight message
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.opts.MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.opts.MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// 消息处理完毕，从InFlight中移除
// 这个消息是remote给出的，如果在inFlightWorker的defaultWorkerWait毫秒超时之前没有收到FIN，那么inFlightWorker就会看到这个消息并把它标记为超时
// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// 主动requeue一个消息，如果timeout设为0则直接Requeue，否则放入deferred队列。
// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	if timeout == 0 {
		return c.doRequeue(msg)
	}

	// deferred requeue
	return c.StartDeferredTimeout(msg, timeout)
}

// 客户端链接的增删，也是线程安全的map操作

// AddClient adds a client to the Channel's client list
func (c *Channel) AddClient(clientID int64, client Consumer) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return
	}
	c.clients[clientID] = client
}

// 远端关闭连接的时候，client从channel中移除
// RemoveClient removes a client from the Channel's client list
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeralChannel == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

// 链接channel和protocol的入口函数，这里开始，msg绑定了client，拥有了优先级和deliverTS。
// 完成信息补充后，msg被添加到InFlight字典和InFlight优先级队列中（为何要有两套呢？优先级队列也拥有msg的全部信息）
// 答案：字典是用作判断消息是否已经入队列的hash，判断有无的复杂度O(1)，PQ是选择优先级最高的消息，排序复杂度O(log(n))。
// inFlight dict的新增使用msgID做key，删除同理并校验clientID。inFlight PQ的增删传入message，内部使用message结构预留的index做线型索引。
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// deferred与InFlight相比，除了消息自身的优先级外又附加了新的优先级，同样是UNIXnano时间戳+timeout值。
// 这里因为msg外围新包裹了内容，因此容器类型也变成了*pqueue.Item
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
	// wrap成为Item类型
	item := &pqueue.Item{Value: msg, Priority: absTs}
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)
	return nil
}

// 把来自InFlight或者deferred队列的消息移动回incoming队列

// doRequeue performs the low level operations to requeue a message
func (c *Channel) doRequeue(msg *Message) error {
	c.RLock()
	defer c.RUnlock()
	if atomic.LoadInt32(&c.exitFlag) == 1 {
		return errors.New("exiting")
	}
	c.incomingMsgChan <- msg
	atomic.AddUint64(&c.requeueCount, 1)
	return nil
}

// 以下7个函数，原子性操作了InFlight和deferred字典和优先级队列(字典的增加/删除，自实现PQ的add和remove，heap实现PQ的add)

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.Unlock()
	return nil
}

// 从inFlight队列删除时会校验传入的clientID与消息是否匹配，只有匹配才能移除
// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.Unlock()
	return msg, nil
}

func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.Lock()
	defer c.Unlock()
// 取msgID之前要先转换回message类型

	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item

	return nil
}

func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.Lock()
	defer c.Unlock()

	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)

	return item, nil
}

// 为什么这里插入了deferredPQ但是没看到删除：PeekAndShift在PQ内部实现为public函数了
func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	defer c.deferredMutex.Unlock()
	// 直接用heap库，内部就不用自己sift了
	heap.Push(&c.deferredPQ, item)
}

// Router handles the muxing of incoming Channel messages, either writing
// to the in-memory channel or to the backend
func (c *Channel) router() {
	var msgBuf bytes.Buffer
	for msg := range c.incomingMsgChan {
		select {
		case c.memoryMsgChan <- msg:
		default:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.l.Output(2, fmt.Sprintf(
					"CHANNEL(%s) ERROR: failed to write message to backend - %s",
					c.name, err))
				c.ctx.nsqd.SetHealth(err)
			}
		}
	}

	c.ctx.l.Output(2, fmt.Sprintf(
		"CHANNEL(%s): closing ... router", c.name))
}

// messagePump reads messages from either memory or backend and writes
// to the client output go channel
//
// it is also performs in-flight accounting and initiates the auto-requeue
// goroutine
func (c *Channel) messagePump() {
	var msg *Message
	var buf []byte
	var err error

	for {
		// do an extra check for closed exit before we select on all the memory/backend/exitChan
		// this solves the case where we are closed and something else is draining clientMsgChan into
		// backend. we don't want to reverse that
		if atomic.LoadInt32(&c.exitFlag) == 1 {
			goto exit
		}

		select {
		case msg = <-c.memoryMsgChan:
		case buf = <-c.backend.ReadChan():
			msg, err = decodeMessage(buf)
			if err != nil {
				c.ctx.l.Output(2, fmt.Sprintf(
					"ERROR: failed to decode message - %s", err))
				continue
			}
		case <-c.exitChan:
			goto exit
		}

		msg.Attempts++

		atomic.StoreInt32(&c.bufferedCount, 1)
		c.clientMsgChan <- msg
		atomic.StoreInt32(&c.bufferedCount, 0)
		// the client will call back to mark as in-flight w/ it's info
	}

exit:
	c.ctx.l.Output(2, fmt.Sprintf("CHANNEL(%s): closing ... messagePump", c.name))
	close(c.clientMsgChan)
}

func (c *Channel) deferredWorker() {
	// 见下面pqWorker注释
	c.pqWorker(&c.deferredPQ, &c.deferredMutex, func(item *pqueue.Item) {
		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			return
		}
		c.doRequeue(msg)
	})
}

// 取得最高优先级的Inflightmsg
// 理论上如果remote处理消息足够快，所有消息都会在defaultWorkerWait的时间内收到FIN并从inFlight移除，不会进入这个处理
func (c *Channel) inFlightWorker() {
	ticker := time.NewTicker(defaultWorkerWait)
	for {
		select {
		case <-ticker.C:
		case <-c.exitChan:
			goto exit
		}
		now := time.Now().UnixNano()
		for {
			c.inFlightMutex.Lock()
			// 取优先级队列0元素，now在这里用来判断有效性，如果0元素的pri比now还大，就返回nil
			msg, _ := c.inFlightPQ.PeekAndShift(now)
			c.inFlightMutex.Unlock()

			if msg == nil {
				break
			}

			_, err := c.popInFlightMessage(msg.clientID, msg.ID)
			if err != nil {
				break
			}
		// 只要有InFlight消息，timeout计数就+1，而且会把消息Requeue：只要这里拿到的，就是超时的，需要重新计算pri并排队
		// 未超时的都已经取走了
			atomic.AddUint64(&c.timeoutCount, 1)
			c.RLock()
			client, ok := c.clients[msg.clientID]
			c.RUnlock()
			if ok {
				// 有client绑定的消息，通知client修改inFlight计数器。
				// not ok的情况：处理这个消息的remote关闭了连接，IOLoop移除client并退出。
				client.TimedOutMessage()
			}
			c.doRequeue(msg)
		}
	}

exit:
	c.ctx.l.Output(2, fmt.Sprintf("CHANNEL(%s): closing ... inFlightWorker", c.name))
	ticker.Stop()
}


// deferred队列中的消息和inFlight中超时的消息处理类似，但使用callback来决定这些消息最后怎么办，而不是统一requeue了事。
// 虽然deferredWorker最终提供的cb函数就是个Requeue，好吧。。。

// generic loop (executed in a goroutine) that periodically wakes up to walk
// the priority queue and call the callback
func (c *Channel) pqWorker(pq *pqueue.PriorityQueue, mutex *sync.Mutex, callback func(item *pqueue.Item)) {
	ticker := time.NewTicker(defaultWorkerWait)
	for {
		select {
		case <-ticker.C:
		case <-c.exitChan:
			goto exit
		}
		now := time.Now().UnixNano()
		for {
			mutex.Lock()
			item, _ := pq.PeekAndShift(now)
			mutex.Unlock()

			if item == nil {
				break
			}

			callback(item)
		}
	}

exit:
	c.ctx.l.Output(2, fmt.Sprintf("CHANNEL(%s): closing ... pqueue worker", c.name))
	ticker.Stop()
}
