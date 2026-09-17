package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"marketpipe/internal/binance"
	"marketpipe/internal/publisher"
	"marketpipe/internal/stream"
)

type Config struct {
	Binance       binance.Config   `json:"binance"`
	WebSocket     stream.Config    `json:"websocket"`
	ZMQ           publisher.Config `json:"zmq"`
	QueueCapacity int              `json:"queue_capacity"`
	HTTPListen    string           `json:"http_listen"`
}

func Load(path string) (Config, error) {
	c := Config{Binance: binance.Defaults(), WebSocket: stream.Defaults(), ZMQ: publisher.Config{Endpoint: "tcp://127.0.0.1:5556", SendHWM: 10000}, QueueCapacity: 32768, HTTPListen: "127.0.0.1:8080"}
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return c, errors.New("trailing JSON content")
	}
	if err := c.Binance.Validate(); err != nil {
		return c, err
	}
	if err := c.WebSocket.Validate(); err != nil {
		return c, err
	}
	if err := c.ZMQ.Validate(); err != nil {
		return c, err
	}
	if c.QueueCapacity < 1 || c.QueueCapacity > 1000000 {
		return c, errors.New("queue_capacity must be in 1..1000000")
	}
	if _, _, err := net.SplitHostPort(c.HTTPListen); err != nil {
		return c, fmt.Errorf("http_listen: %w", err)
	}
	return c, nil
}
