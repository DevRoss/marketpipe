"""pip install pyzmq; python examples/subscriber.py
Demonstration only: printing every tick is not appropriate on a trading hot path.
"""
import argparse
import json
import sys
import time
import zmq


def main():
    parser = argparse.ArgumentParser()
    endpoints = {"quote": 5556, "trade": 5557, "agg_trade": 5558, "heartbeat": 5559}
    parser.add_argument("--kind", choices=endpoints, default="quote")
    parser.add_argument("--endpoint", help="override the selected kind endpoint")
    parser.add_argument("--topic", default="md.v1.")
    args = parser.parse_args()
    context = zmq.Context()
    sub = context.socket(zmq.SUB)
    sub.setsockopt(zmq.LINGER, 0)
    sub.setsockopt(zmq.RCVHWM, 10000)
    sub.setsockopt(zmq.MAXMSGSIZE, 1024 * 1024)
    sub.setsockopt_string(zmq.SUBSCRIBE, args.topic)
    sub.connect(args.endpoint or f"tcp://127.0.0.1:{endpoints[args.kind]}")
    last = {}
    try:
        while True:
            if not sub.poll(5000):
                print("No messages on selected channel for 5s; check heartbeat or HTTP health", file=sys.stderr)
                continue
            topic, payload = sub.recv_multipart()
            event = json.loads(payload)
            if event["schema"] != 1:
                raise ValueError("unsupported schema")
            seq = int(event["seq"])
            session = event.get("source_session")
            prev = last.get(topic)
            if prev:
                if prev[0] != event["instance"]:
                    print("Gateway restarted", topic, file=sys.stderr)
                elif seq != prev[1] + 1:
                    print("Gap/reorder", topic, prev[1], seq, file=sys.stderr)
                if prev[2] != session:
                    print("Source reconnected; possible upstream gap", topic, file=sys.stderr)
            last[topic] = event["instance"], seq, session
            # Use Decimal(event['quote']['bid_price']) for exact decimal arithmetic.
            # recv timestamp is a string to preserve ns precision in JavaScript.
            local_age_ms = (time.time_ns() - int(event["receive_time_ns"])) / 1e6
            print(topic.decode(), f"local_age_ms={local_age_ms:.3f}", event)
    except KeyboardInterrupt:
        pass
    finally:
        sub.close()
        context.term()


if __name__ == "__main__":
    main()
