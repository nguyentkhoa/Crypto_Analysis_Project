# bitget_ws.py
import json, datetime, threading, time, websocket
import pandas as pd
class BitgetWS:
    """
    Cho phép subscribe nhiều channel (ticker, candle1m, trade, …) một cách tái sử dụng.
    - inst_type : 'SPOT', 'MIX', 'USDS-FUTURES', …
    - channel   : 'ticker', 'candle1m', 'trade', …
    - inst_ids  : list các cặp coin (BTCUSDT, ETHUSDT,…)
    - dur_sec   : thời gian chạy (giây) trước khi tự động ngắt kết nối
    """
    WS_URL = "wss://ws.bitget.com/v2/ws/public"
    def __init__(self, inst_type, channel, inst_ids, dur_sec=5):
        self.inst_type = inst_type
        self.channel   = channel
        self.inst_ids  = inst_ids
        self.dur_sec   = dur_sec
        # nơi lưu tất cả bản ghi; về sau chỉ việc pd.DataFrame(self.data)
        self.data = []

        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            on_open    = self._on_open,
            on_message = self._on_message,
            on_error   = lambda ws, err: print("❌  Error:", err),
            on_close   = lambda ws, code, msg: print("🔌  Closed")
        )
# ----------  CALLBACKS  ----------
    def _on_open(self, ws):
        for inst in self.inst_ids:
            req = {
                "op": "subscribe",
                "args": [{
                    "instType": self.inst_type,
                    "channel" : self.channel,
                    "instId"  : inst
                }]
            }
            ws.send(json.dumps(req))
            print(f"✅  Subscribed {inst} → {self.channel}")

    def _on_message(self, ws, msg_str):
        msg  = json.loads(msg_str)
        rows = msg.get("data", [])            
        if not rows:
            return
        for r in rows:
            if isinstance(r,dict):
                r["instId"]  = msg["arg"]["instId"]        
                r["channel"] = self.channel                
                r['type']=self.inst_type
                r["ts_recv"] = datetime.datetime.now().isoformat()
                self.data.append(r)
            elif isinstance(r,list):
                self.data.append(
                    {
                    "instId"  : msg.get("arg", {}).get("instId", ""),
                    "channel" : self.channel,
                    'type':self.inst_type,
                    "ts_recv" : datetime.datetime.now().isoformat(),
                    "start_time"  : r[0],
                    "open_price":r[1],
                    "highest_price":r[2],
                    "lowest_price":r[3],
                    "closing_price":r[4],
                    "trading_volume_coin":r[5],
                    "trading_volume_usd":r[6],
                    }
                )
    def run(self):
        """Mở WS, chạy dur_sec giây, đóng lại, trả DataFrame"""
        t = threading.Thread(target=self.ws.run_forever, daemon=True)
        t.start()
        time.sleep(self.dur_sec)
        self.ws.close()
        t.join()
        return pd.DataFrame(self.data)
