"""Solana event stream monitor: research only, never submits transactions."""
import json, os, threading, time, datetime, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from websocket import create_connection, WebSocketTimeoutException

RPC_WS = os.getenv('SOLANA_WS_URL', 'wss://api.mainnet-beta.solana.com/')
# Raydium AMM v4; logs are transaction events, NOT verified pool launches.
PROGRAM = os.getenv('SOLANA_PROGRAM_ID', '675kPX9MHTjS2zt1qfr1NYHuze1uFQd1f4Gd5Kp9a')
STATE = {'connected': False, 'last_event': None, 'events': 0, 'last_error': None, 'mode': 'research_only'}
LOCK = threading.Lock()

def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def listen():
    retry = 2
    while True:
        ws = None
        try:
            ws = create_connection(RPC_WS, timeout=30)
            ws.settimeout(45)
            ws.send(json.dumps({'jsonrpc':'2.0','id':1,'method':'logsSubscribe','params':[{'mentions':[PROGRAM]},{'commitment':'confirmed'}]}))
            ack = json.loads(ws.recv())
            if 'result' not in ack:
                raise RuntimeError(f'subscription rejected: {str(ack)[:350]}')
            with LOCK:
                STATE.update(connected=True, last_error=None)
            retry = 2
            while True:
                msg = json.loads(ws.recv())
                if msg.get('method') != 'logsNotification':
                    continue
                value = msg.get('params',{}).get('result',{}).get('value',{})
                with LOCK:
                    STATE['events'] += 1
                    STATE['last_event'] = utc()
                # Emit signature only. No false claim that every event is a new token.
                print(json.dumps({'ts':utc(),'event':'program_transaction','signature':value.get('signature'),'err':value.get('err')}),flush=True)
        except Exception as exc:
            with LOCK:
                STATE.update(connected=False,last_error=str(exc))
            print('stream_error',str(exc),flush=True)
            time.sleep(retry)
            retry = min(60,retry*2)
        finally:
            if ws:
                try: ws.close()
                except Exception: pass

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ('/','/health'):
            self.send_error(404);return
        with LOCK: payload = dict(STATE)
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def log_message(self,*args):pass

if __name__=='__main__':
    threading.Thread(target=listen,daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),Handler).serve_forever()
