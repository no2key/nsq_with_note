package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/bitly/nsq/util"
)

type NSQAdmin struct {
	opts          *nsqadminOptions
	httpAddr      *net.TCPAddr
	httpListener  net.Listener
	waitGroup     util.WaitGroupWrapper
	notifications chan *AdminAction
}

func NewNSQAdmin(opts *nsqadminOptions) *NSQAdmin {
	if len(opts.NSQDHTTPAddresses) == 0 && len(opts.NSQLookupdHTTPAddresses) == 0 {
		log.Fatalf("--nsqd-http-address or --lookupd-http-address required.")
	}

	if len(opts.NSQDHTTPAddresses) != 0 && len(opts.NSQLookupdHTTPAddresses) != 0 {
		log.Fatalf("use --nsqd-http-address or --lookupd-http-address not both")
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", opts.HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	n := &NSQAdmin{
		opts:          opts,
		httpAddr:      httpAddr,
		notifications: make(chan *AdminAction),
	}

	n.opts.Logger.Output(2, util.Version("nsqlookupd"))

	return n
}

func (n *NSQAdmin) handleAdminActions() {
	for action := range n.notifications {
		content, err := json.Marshal(action)
		if err != nil {
			log.Printf("Error serializing admin action! %s", err)
		}
		httpclient := &http.Client{Transport: util.NewDeadlineTransport(10 * time.Second)}
		log.Printf("Posting notification to %s", *notificationHTTPEndpoint)
		_, err = httpclient.Post(*notificationHTTPEndpoint, "application/json", bytes.NewBuffer(content))
		if err != nil {
			log.Printf("Error posting notification: %s", err)
		}
	}
}

func (n *NSQAdmin) Main() {
	httpListener, err := net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.httpAddr, err.Error())
	}
	n.httpListener = httpListener
	httpServer := NewHTTPServer(&Context{n})
	n.waitGroup.Wrap(func() {
		util.HTTPServer(n.httpListener, httpServer, n.opts.Logger, "HTTP")
	})
	n.waitGroup.Wrap(func() { n.handleAdminActions() })
}

func (n *NSQAdmin) Exit() {
	n.httpListener.Close()
	close(n.notifications)
	n.waitGroup.Wait()
}
