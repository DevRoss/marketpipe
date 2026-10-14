package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"marketpipe/internal/binance"
	"marketpipe/internal/config"
	"marketpipe/internal/hub"
	"marketpipe/internal/monitor"
	"marketpipe/internal/publisher"
	"marketpipe/internal/stream"
	"marketpipe/pkg/market"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	path := flag.String("config", "configs/local.json", "JSON configuration")
	flag.Parse()
	if err := run(*path); err != nil {
		slog.Error("marketd stopped", "error", err)
		os.Exit(1)
	}
}
func run(path string) error {
	c, err := config.Load(path)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stats := monitor.New()
	h := hub.NewRouter(c.QueueCapacity, c.Kinds(), stats)
	pubCtx, cancelPub := context.WithCancel(context.Background())
	defer cancelPub()
	ready := make(chan error, 1)
	pubDone := make(chan error, 1)
	go func() { pubDone <- publisher.RunAll(pubCtx, c.ZMQ, h.Inputs(), stats, ready) }()
	if err := <-ready; err != nil {
		<-pubDone
		return fmt.Errorf("publisher start: %w", err)
	}
	ln, err := net.Listen("tcp", c.HTTPListen)
	if err != nil {
		cancelPub()
		<-pubDone
		return err
	}
	server := &http.Server{Handler: stats.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	errorsCh := make(chan error, 4)
	httpDone := make(chan struct{})
	go func() {
		defer close(httpDone)
		if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errorsCh <- err
		}
	}()
	adapters := []market.Adapter{&binance.Adapter{Config: c.Binance, Manager: &stream.Manager{Config: c.WebSocket, Stats: stats}}}
	var producers sync.WaitGroup
	for _, a := range adapters {
		producers.Add(1)
		go func(a market.Adapter) {
			defer producers.Done()
			if err := a.Run(ctx, h); err != nil {
				errorsCh <- fmt.Errorf("adapter %s: %w", a.Name(), err)
			}
		}(a)
	}
	producers.Add(1)
	go func() {
		defer producers.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				h.Emit(market.Event{Exchange: "system", Market: "gateway", Symbol: "ALL", Kind: "heartbeat", EventTimeMS: now.UnixMilli(), ReceiveTimeNS: now.UnixNano(), Connections: stats.Connections()})
			}
		}
	}()
	slog.Info("marketd started", "zmq", c.ZMQ.Endpoint, "http", c.HTTPListen)
	pubExited := false
	select {
	case <-ctx.Done():
	case err = <-errorsCh:
	case err = <-pubDone:
		pubExited = true
		if err == nil {
			err = errors.New("publisher unexpectedly stopped")
		}
	}
	cancel()
	producers.Wait()
	h.Close()
	if !pubExited {
		t := time.NewTimer(3 * time.Second)
		select {
		case pErr := <-pubDone:
			if err == nil {
				err = pErr
			}
		case <-t.C:
			cancelPub()
			<-pubDone
			slog.Warn("publisher drain deadline exceeded")
		}
		t.Stop()
	}
	shutdown, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()
	if e := server.Shutdown(shutdown); e != nil {
		server.Close()
	}
	<-httpDone
	slog.Info("marketd stopped", "received", stats.TotalReceived(), "ingress_dropped", stats.TotalDropped(), "publish_calls", stats.TotalSent())
	return err
}
