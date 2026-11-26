"""Upstox Market Data Feed V3: protobuf decoder + WebSocket client (Phase 1: LTP + change %).

Proto: MarketDataFeed.proto (FeedResponse -> feeds map -> Feed -> ltpc).
protoc ki zaroorat nahi: descriptor yahan programmatically banaya gaya hai (same field numbers).
"""
import json
import threading
import time

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

PKG = "com.upstox.marketdatafeederv3udapi.rpc.proto"
_F = descriptor_pb2.FieldDescriptorProto
_OPT, _REP = _F.LABEL_OPTIONAL, _F.LABEL_REPEATED


def _field(name, number, ftype, label=_OPT, type_name=None, oneof=None):
    f = _F(name=name, number=number, type=ftype, label=label)
    if type_name:
        f.type_name = f".{PKG}.{type_name}"
    if oneof is not None:
        f.oneof_index = oneof
    return f


def _build_pool():
    fdp = descriptor_pb2.FileDescriptorProto(name="upstox_marketdatafeed_min.proto", package=PKG, syntax="proto3")
    D, M, E = descriptor_pb2.DescriptorProto, _F, None

    def msg(name, fields, oneofs=None, nested=None):
        m = D(name=name, field=fields)
        for o in oneofs or []:
            m.oneof_decl.add(name=o)
        for n in nested or []:
            m.nested_type.add().CopyFrom(n)
        fdp.message_type.add().CopyFrom(m)

    msg("LTPC", [
        _field("ltp", 1, M.TYPE_DOUBLE), _field("ltt", 2, M.TYPE_INT64), _field("ltq", 3, M.TYPE_INT64),
        _field("cp", 4, M.TYPE_DOUBLE),
    ])
    msg("OHLC", [
        _field("interval", 1, M.TYPE_STRING), _field("open", 2, M.TYPE_DOUBLE), _field("high", 3, M.TYPE_DOUBLE),
        _field("low", 4, M.TYPE_DOUBLE), _field("close", 5, M.TYPE_DOUBLE), _field("vol", 6, M.TYPE_INT64),
        _field("ts", 7, M.TYPE_INT64),
    ])
    msg("MarketOHLC", [_field("ohlc", 1, M.TYPE_MESSAGE, _REP, "OHLC")])
    msg("IndexFullFeed", [_field("ltpc", 1, M.TYPE_MESSAGE, _OPT, "LTPC"),
                          _field("marketOHLC", 2, M.TYPE_MESSAGE, _OPT, "MarketOHLC")])
    # MarketFullFeed: sirf ltpc (field 1) chahiye; baaki fields parser ignore karega
    msg("MarketFullFeed", [_field("ltpc", 1, M.TYPE_MESSAGE, _OPT, "LTPC")])
    msg("FullFeed", [
        _field("marketFF", 1, M.TYPE_MESSAGE, _OPT, "MarketFullFeed", oneof=0),
        _field("indexFF", 2, M.TYPE_MESSAGE, _OPT, "IndexFullFeed", oneof=0),
    ], oneofs=["FullFeedUnion"])
    msg("FirstLevelWithGreeks", [_field("ltpc", 1, M.TYPE_MESSAGE, _OPT, "LTPC")])

    feed_fields = [
        _field("ltpc", 1, M.TYPE_MESSAGE, _OPT, "LTPC", oneof=0),
        _field("fullFeed", 2, M.TYPE_MESSAGE, _OPT, "FullFeed", oneof=0),
        _field("firstLevelWithGreeks", 3, M.TYPE_MESSAGE, _OPT, "FirstLevelWithGreeks", oneof=0),
        _field("requestMode", 4, M.TYPE_ENUM, _OPT, "RequestMode"),
    ]
    msg("Feed", feed_fields, oneofs=["FeedUnion"])

    entry = D(name="FeedsEntry", field=[
        _field("key", 1, M.TYPE_STRING), _field("value", 2, M.TYPE_MESSAGE, _OPT, "Feed"),
    ])
    entry.options.map_entry = True
    msg("FeedResponse", [
        _field("type", 1, M.TYPE_ENUM, _OPT, "Type"),
        _field("feeds", 2, M.TYPE_MESSAGE, _REP, "FeedResponse.FeedsEntry"),
        _field("currentTs", 3, M.TYPE_INT64),
    ], nested=[entry])

    for enum_name, values in (
        ("Type", [("initial_feed", 0), ("live_feed", 1), ("market_info", 2)]),
        ("RequestMode", [("ltpc", 0), ("full_d5", 1), ("option_greeks", 2), ("full_d30", 3)]),
    ):
        en = descriptor_pb2.EnumDescriptorProto(name=enum_name)
        for vname, vnum in values:
            en.value.add(name=vname, number=vnum)
        fdp.enum_type.add().CopyFrom(en)

    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    return pool


_POOL = _build_pool()
FeedResponse = message_factory.GetMessageClass(_POOL.FindMessageTypeByName(f"{PKG}.FeedResponse"))


def parse_frame(raw: bytes) -> dict:
    """Binary frame -> {'type', 'ts', 'feeds': {instrument_key: {'ltp','cp','ltt','ltq'}}}."""
    msg = FeedResponse()
    msg.ParseFromString(raw)
    feeds = {}
    for key, feed in msg.feeds.items():
        if feed.HasField("ltpc"):
            l = feed.ltpc
        elif feed.HasField("fullFeed") and feed.fullFeed.HasField("indexFF"):
            l = feed.fullFeed.indexFF.ltpc
        elif feed.HasField("fullFeed") and feed.fullFeed.HasField("marketFF"):
            l = feed.fullFeed.marketFF.ltpc
        elif feed.HasField("firstLevelWithGreeks"):
            l = feed.firstLevelWithGreeks.ltpc
        else:
            continue
        feeds[key] = {"ltp": l.ltp, "cp": l.cp, "ltt": l.ltt, "ltq": l.ltq}
    return {"type": msg.type, "ts": msg.currentTs, "feeds": feeds}


def change_pct(ltp, cp):
    """Din ka change %: (ltp - cp) / cp * 100. cp 0 ho to None."""
    if not cp:
        return None
    return (ltp - cp) / cp * 100.0


class FeedClient:
    """Background thread: WebSocket se LTP lekar latest() mein rakhta hai. Disconnect par dobara connect karta hai."""

    def __init__(self, ws_url, instrument_keys, mode="ltpc"):
        self._url = ws_url
        self._keys = list(instrument_keys)
        self._mode = mode
        self._lock = threading.Lock()
        self._latest = {}
        self._stop = False
        self.connected = False
        self.last_error = None
        self.attempts = 0
        self.last_event = "start"
        self.msg_count = 0
        self.last_msg_at = None
        threading.Thread(target=self._run, daemon=True).start()

    def status(self):
        return {"connected": self.connected, "attempts": self.attempts, "last_event": self.last_event,
                "msgs": self.msg_count, "last_error": self.last_error}

    def latest(self):
        with self._lock:
            return dict(self._latest)

    def stop(self):
        self._stop = True

    def _run(self):
        import websocket  # websocket-client
        while not self._stop:
            self.attempts += 1
            self.last_event = f"connecting #{self.attempts}"
            app = websocket.WebSocketApp(self._url, on_open=self._on_open, on_message=self._on_message,
                                         on_error=self._on_error, on_close=self._on_close)
            app.run_forever(ping_interval=20, ping_timeout=10)
            self.connected = False
            time.sleep(3)

    def _on_open(self, ws):
        self.connected = True
        self.last_error = None
        self.last_event = "open, subscribe bheja"
        # SUBSCRIBE FORMAT: docs ke hisaab se confirm karna hai (binary/text). Yahan JSON bytes bheje ja rahe hain.
        sub = {"guid": "veer-zone-phase1", "method": "sub",
               "data": {"mode": self._mode, "instrumentKeys": self._keys}}
        import websocket
        ws.send(json.dumps(sub).encode("utf-8"), opcode=websocket.ABNF.OPCODE_BINARY)

    def _on_message(self, ws, message):
        self.msg_count += 1
        self.last_msg_at = time.time()
        if isinstance(message, str):
            self.last_error = f"text frame: {message[:120]}"
            return
        try:
            data = parse_frame(message)
        except Exception as e:  # kharab frame ignore
            self.last_error = f"decode: {e}"
            return
        with self._lock:
            self._latest.update(data["feeds"])

    def _on_error(self, ws, error):
        self.last_error = str(error)[:200]
        self.last_event = "error"

    def _on_close(self, ws, code, msg):
        self.connected = False
        self.last_event = f"closed code={code} msg={msg}"
        if code not in (None, 1000):
            self.last_error = f"closed code={code} msg={str(msg)[:120]}"
