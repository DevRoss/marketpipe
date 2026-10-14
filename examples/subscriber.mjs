// npm install zeromq; node examples/subscriber.mjs
import { Subscriber } from "zeromq";
const sub = new Subscriber({ receiveHighWaterMark: 10000, linger: 0 });
sub.connect(process.env.ZMQ_ENDPOINT || "tcp://127.0.0.1:5556");
sub.subscribe(process.env.ZMQ_TOPIC ?? "md.v1.binance.usdm.BTCUSDT.quote|");
const last = new Map();
process.on("SIGINT", () => sub.close());
try {
  for await (const [topic, payload] of sub) {
    const e = JSON.parse(payload.toString());
    if (e.schema !== 1) throw new Error("unsupported schema");
    const key = topic.toString();
    const seq = BigInt(e.seq); // Do not convert seq or ns timestamps to Number.
    const prev = last.get(key);
    if (prev && (prev.instance !== e.instance || seq !== prev.seq + 1n || prev.session !== e.source_session)) {
      console.error("Restart, source reconnect, or gap:", key);
    }
    last.set(key, { instance: e.instance, seq, session: e.source_session });
    console.log(key, e);
  }
} finally { sub.close(); }
