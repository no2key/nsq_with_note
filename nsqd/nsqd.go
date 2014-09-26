package nsqd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/lookupd"
)

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64

	sync.RWMutex

	opts *nsqdOptions

	healthy int32
	err     error

	topicMap map[string]*Topic

	lookupPeers []*lookupPeer

	tcpAddr       *net.TCPAddr
	httpAddr      *net.TCPAddr
	httpsAddr     *net.TCPAddr
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	tlsConfig     *tls.Config

	idChan     chan MessageID
	notifyChan chan interface{}
	exitChan   chan int
	waitGroup  util.WaitGroupWrapper
}

func NewNSQD(opts *nsqdOptions) *NSQD {
	var httpsAddr *net.TCPAddr
// 参数校验开始
	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		log.Fatalf("--max-deflate-level must be [1,9]")
	}

	if opts.ID < 0 || opts.ID >= 4096 {
		log.Fatalf("--worker-id must be [0,4096)")
	}
// 结束

// 三个监听地址
	tcpAddr, err := net.ResolveTCPAddr("tcp", opts.TCPAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", opts.HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	if opts.HTTPSAddress != "" {
		httpsAddr, err = net.ResolveTCPAddr("tcp", opts.HTTPSAddress)
		if err != nil {
			log.Fatal(err)
		}
	}
// TCP，HTTP和HTTPS三个监听地址处理完成

// 由IP:Port生成一个标志
	if opts.StatsdPrefix != "" {
		statsdHostKey := util.StatsdHostKey(net.JoinHostPort(opts.BroadcastAddress,
			strconv.Itoa(httpAddr.Port)))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	n := &NSQD{
		opts:       opts,
		healthy:    1,
		tcpAddr:    tcpAddr,
		httpAddr:   httpAddr,
		httpsAddr:  httpsAddr,
		topicMap:   make(map[string]*Topic),
		idChan:     make(chan MessageID, 4096),
		exitChan:   make(chan int),
		notifyChan: make(chan interface{}),
		tlsConfig:  buildTLSConfig(opts),		// 在这里加载TLS密钥信息
	}

	n.waitGroup.Wrap(func() { n.idPump() })

	n.opts.Logger.Output(2, util.Version("nsqd"))
	n.opts.Logger.Output(2, fmt.Sprintf("ID: %d", n.opts.ID))

	return n
}

func (n *NSQD) SetHealth(err error) {
	n.Lock()
	defer n.Unlock()
	n.err = err
	if err != nil {
		atomic.StoreInt32(&n.healthy, 0)
	} else {
		atomic.StoreInt32(&n.healthy, 1)
	}
}

func (n *NSQD) IsHealthy() bool {
	return atomic.LoadInt32(&n.healthy) == 1
}

func (n *NSQD) GetError() error {
	n.RLock()
	defer n.RUnlock()
	return n.err
}

func (n *NSQD) GetHealth() string {
	if !n.IsHealthy() {
		return fmt.Sprintf("NOK - %s", n.GetError())
	}
	return "OK"
}

func (n *NSQD) Main() {
	var httpListener net.Listener
	var httpsListener net.Listener
// 向监听服务器添加logger
	ctx := &context{n, n.opts.Logger}

	if n.opts.TLSClientAuthPolicy != "" {
		n.opts.TLSRequired = true
	}

	if n.tlsConfig == nil && n.opts.TLSRequired {
		log.Fatalf("FATAL: cannot require TLS client connections without TLS key and cert")
	}

	n.waitGroup.Wrap(func() { n.lookupLoop() })
// tcpListener用来监听端口，tcpServer用来Handle客户端的clientConnection，TCPServer将循环accept前者的请求并交由后者处理。
	tcpListener, err := net.Listen("tcp", n.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.tcpAddr, err.Error())
	}
	n.tcpListener = tcpListener
	tcpServer := &tcpServer{ctx: ctx}
	n.waitGroup.Wrap(func() {
		util.TCPServer(n.tcpListener, tcpServer, n.opts.Logger)
	})
// HTTPS用tls配置建立listener，server使用httpserver加tls使能
	if n.tlsConfig != nil && n.httpsAddr != nil {
		httpsListener, err = tls.Listen("tcp", n.httpsAddr.String(), n.tlsConfig)
		if err != nil {
			log.Fatalf("FATAL: listen (%s) failed - %s", n.httpsAddr, err.Error())
		}
		n.httpsListener = httpsListener
		httpsServer := &httpServer{
			ctx:         ctx,
			tlsEnabled:  true,
			tlsRequired: true,
		}
		n.waitGroup.Wrap(func() {
			util.HTTPServer(n.httpsListener, httpsServer, n.opts.Logger, "HTTPS")
		})
	}
// HTTP服务
	httpListener, err = net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.httpAddr, err.Error())
	}
	n.httpListener = httpListener
	httpServer := &httpServer{
		ctx:         ctx,
		tlsEnabled:  false,
		tlsRequired: n.opts.TLSRequired,
	}
	n.waitGroup.Wrap(func() {
		util.HTTPServer(n.httpListener, httpServer, n.opts.Logger, "HTTP")
	})

	if n.opts.StatsdAddress != "" {
		n.waitGroup.Wrap(func() { n.statsdLoop() })
	}
}

// 取出meta中保存的topic列表，检查名称有效性并新建topic恢复现场，对每个topic同样恢复其channel
func (n *NSQD) LoadMetadata() {
	fn := fmt.Sprintf(path.Join(n.opts.DataPath, "nsqd.%d.dat"), n.opts.ID)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			n.opts.Logger.Output(2, fmt.Sprintf(
				"ERROR: failed to read channel metadata from %s - %s", fn, err))
		}
		return
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		n.opts.Logger.Output(2, fmt.Sprintf("ERROR: failed to parse metadata - %s", err))
		return
	}

	topics, err := js.Get("topics").Array()
	if err != nil {
		n.opts.Logger.Output(2, fmt.Sprintf("ERROR: failed to parse metadata - %s", err))
		return
	}

	for ti := range topics {
		topicJs := js.Get("topics").GetIndex(ti)

		topicName, err := topicJs.Get("name").String()
		if err != nil {
			n.opts.Logger.Output(2, fmt.Sprintf("ERROR: failed to parse metadata - %s", err))
			return
		}
		if !util.IsValidTopicName(topicName) {
			n.opts.Logger.Output(2, fmt.Sprintf(
				"WARNING: skipping creation of invalid topic %s", topicName))
			continue
		}
		topic := n.GetTopic(topicName)

		paused, _ := topicJs.Get("paused").Bool()
		if paused {
			topic.Pause()
		}

		channels, err := topicJs.Get("channels").Array()
		if err != nil {
			n.opts.Logger.Output(2, fmt.Sprintf("ERROR: failed to parse metadata - %s", err))
			return
		}

		for ci := range channels {
			channelJs := topicJs.Get("channels").GetIndex(ci)

			channelName, err := channelJs.Get("name").String()
			if err != nil {
				n.opts.Logger.Output(2, fmt.Sprintf("ERROR: failed to parse metadata - %s", err))
				return
			}
			if !util.IsValidChannelName(channelName) {
				n.opts.Logger.Output(2, fmt.Sprintf(
					"WARNING: skipping creation of invalid channel %s", channelName))
				continue
			}
			channel := topic.GetChannel(channelName)

			paused, _ = channelJs.Get("paused").Bool()
			if paused {
				channel.Pause()
			}
		}
	}
}

func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fileName := fmt.Sprintf(path.Join(n.opts.DataPath, "nsqd.%d.dat"), n.opts.ID)
	n.opts.Logger.Output(2, fmt.Sprintf("NSQ: persisting topic/channel metadata to %s", fileName))

	js := make(map[string]interface{})
	topics := make([]interface{}, 0)
	for _, topic := range n.topicMap {
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := make([]interface{}, 0)
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if !channel.ephemeralChannel {
				channelData := make(map[string]interface{})
				channelData["name"] = channel.name
				channelData["paused"] = channel.IsPaused()
				channels = append(channels, channelData)
			}
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = util.BINARY_VERSION
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fileName + ".tmp"
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	err = atomic_rename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

func (n *NSQD) Exit() {
// 退出时先关闭监听
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}
// 持久化数据
	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.opts.Logger.Output(2, fmt.Sprintf("ERROR: failed to persist metadata - %s", err))
	}
	n.opts.Logger.Output(2, "NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(n.exitChan)
// 关闭exitChan会使得等待这个chan的routine得到信号，然后等待三个监听彻底关闭和idPump完成
	n.waitGroup.Wait()
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)

// GetTopic会新建不存在的topic。
func (n *NSQD) GetTopic(topicName string) *Topic {
	n.Lock()
	t, ok := n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	} else {
		t = NewTopic(topicName, &context{n, n.opts.Logger})
		n.topicMap[topicName] = t

		n.opts.Logger.Output(2, fmt.Sprintf("TOPIC(%s): created", t.name))

		// release our global nsqd lock, and switch to a more granular topic lock while we init our
		// channels from lookupd. This blocks concurrent PutMessages to this topic.
		t.Lock()
		n.Unlock()
		// if using lookupd, make a blocking call to get the topics, and immediately create them.
		// this makes sure that any message received is buffered to the right channels

// Lookup部分待看
		if len(n.lookupPeers) > 0 {
			channelNames, _ := lookupd.GetLookupdTopicChannels(t.name, n.lookupHttpAddrs())
			for _, channelName := range channelNames {
				t.getOrCreateChannel(channelName)
			}
		}
		t.Unlock()

		// NOTE: I would prefer for this to only happen in topic.GetChannel() but we're special
		// casing the code above so that we can control the locks such that it is impossible
		// for a message to be written to a (new) topic while we're looking up channels
		// from lookupd...
		//
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}
	return t
}


// GetExistingTopic不会新建不存在的Topic
// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering

// 先删除topic再解除注册，否则新来的请求会自动注册topic从而错误delete后创建的topic
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

func (n *NSQD) idPump() {

// 循环生产全局唯一id，并注入idChan供其他routine使用
	factory := &guidFactory{}
	lastError := time.Now()
	for {
		id, err := factory.NewGUID(n.opts.ID)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				n.opts.Logger.Output(2, fmt.Sprintf("ERROR: %s", err))
				lastError = now
			}
			runtime.Gosched()
			continue
		}
		select {
		case n.idChan <- id.Hex():
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.opts.Logger.Output(2, "ID: closing")
}

// 手动notify的时候持久化数据，通知可能来自topic的new或exit函数
func (n *NSQD) Notify(v interface{}) {
	// by selecting on exitChan we guarantee that
	// we do not block exit, see issue #123
	select {
	case <-n.exitChan:
	case n.notifyChan <- v:
		n.Lock()
		err := n.PersistMetadata()
		if err != nil {
			n.opts.Logger.Output(2, fmt.Sprintf("ERROR: failed to persist metadata - %s", err))
		}
		n.Unlock()
	}
}

func buildTLSConfig(opts *nsqdOptions) *tls.Config {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		log.Fatalf("ERROR: failed to LoadX509KeyPair %s", err.Error())
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		ca_cert_file, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			log.Fatalf("ERROR: failed to read custom Certificate Authority file %s", err.Error())
		}
		if !tlsCertPool.AppendCertsFromPEM(ca_cert_file) {
			log.Fatalf("ERROR: failed to append certificates from Certificate Authority file")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.opts.AuthHTTPAddresses) != 0
}
