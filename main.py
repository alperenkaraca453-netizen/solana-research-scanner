"""Raydium AMM v4 pool-init research monitor. No orders, keys, or trading."""
import collections, datetime, json, os, queue, threading, time, urllib.request, urllib.error, re
from urllib.parse import urlsplit
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from websocket import create_connection

PROGRAM = os.getenv('SOLANA_PROGRAM_ID', '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8')
WS_URL = os.getenv('SOLANA_WS_URL', 'wss://api.mainnet-beta.solana.com/')
HTTP_URL = os.getenv('SOLANA_HTTP_URL', 'https://api.mainnet-beta.solana.com')
MAX_RPM = max(1, min(60, int(os.getenv('RPC_MAX_RPM', '12'))))
SAMPLE_EVERY = max(1, int(os.getenv('SAMPLE_EVERY', '250'))) # bounded RPC sampling
QUEUE = queue.Queue(maxsize=250)
LOCK = threading.Lock()
STATE = dict(connected=False, subscription_id=None, last_event=None, last_error=None, events=0,
             candidates=0, decoded=0, rpc_errors=0, dropped=0, filtered=0,
             failed_transactions=0, init_markers=0, duplicates=0, rpc_null=0,
             sampled=0, verified_inits=0, non_init_decoded=0,
             last_candidate=None, mode='research_only', live_trading=False)
CANDIDATES = collections.deque(maxlen=30)
SEEN = collections.deque(maxlen=1000)
SEEN_SET = set()

def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def emit(event, **kwargs): print(json.dumps({'ts':now(),'event':event,**kwargs}, default=str), flush=True)
def update(**kwargs):
    with LOCK: STATE.update(kwargs)
def bump(key):
    with LOCK: STATE[key] += 1

# Anchor to Raydium's own program log, not arbitrary swap instructions.
# Logs can contain "initialize2" or "initialize_2" depending on the program build.
INIT_RE = re.compile(r"\binitialize[_ ]?2\b|\binitializeinstruction2\b", re.I)

def enqueue(value):
    sig = value.get('signature')
    if not sig: return
    if value.get('err') is not None:
        bump('failed_transactions'); return
    logs = value.get('logs') or []
    marker = any(INIT_RE.search(str(line)) for line in logs)
    with LOCK:
        if marker: STATE['init_markers'] += 1
        else: STATE['filtered'] += 1
        # Sample transactions even without matching log text. Bounded by
        # SAMPLE_EVERY to protect the public RPC endpoint.
        sample = (STATE['events'] % SAMPLE_EVERY == 0)
        if not (marker or sample): return
        if sig in SEEN_SET:
            STATE['duplicates'] += 1; return
        if len(SEEN) == SEEN.maxlen:
            SEEN_SET.discard(SEEN.popleft())
        SEEN.append(sig); SEEN_SET.add(sig)
        if sample and not marker: STATE['sampled'] += 1
    try:
        QUEUE.put_nowait((sig, marker))
        if marker: bump('candidates')
        emit('transaction_queued',signature=sig,reason='init_marker' if marker else 'sample')
    except queue.Full:
        bump('dropped')

def stream():
    delay=2
    while True:
        ws=None
        try:
            ws=create_connection(WS_URL, timeout=25); ws.settimeout(50)
            ws.send(json.dumps({'jsonrpc':'2.0','id':1,'method':'logsSubscribe','params':[{'mentions':[PROGRAM]},{'commitment':'confirmed'}]}))
            ack=json.loads(ws.recv())
            if not isinstance(ack.get('result'),int): raise RuntimeError('subscription rejected: '+str(ack)[:250])
            update(connected=True,subscription_id=ack['result'],last_error=None)
            emit('subscribed',program=PROGRAM,subscription=ack['result'])
            delay=2
            while True:
                msg=json.loads(ws.recv())
                if msg.get('method') != 'logsNotification': continue
                value=msg.get('params',{}).get('result',{}).get('value',{})
                with LOCK:
                    STATE['events']+=1; STATE['last_event']=now()
                enqueue(value)
        except Exception as exc:
            update(connected=False,subscription_id=None,last_error=str(exc))
            emit('stream_error',error=str(exc)); time.sleep(delay); delay=min(60,delay*2)
        finally:
            if ws:
                try: ws.close()
                except Exception: pass

def rpc(sig):
    payload=json.dumps({'jsonrpc':'2.0','id':1,'method':'getTransaction',
      'params':[sig,{'encoding':'jsonParsed','commitment':'confirmed','maxSupportedTransactionVersion':0}]}).encode()
    req=urllib.request.Request(HTTP_URL,data=payload,headers={'Content-Type':'application/json','User-Agent':'solana-research-v04'})
    with urllib.request.urlopen(req,timeout=18) as res: answer=json.load(res)
    if 'error' in answer: raise RuntimeError(str(answer['error'])[:220])
    return answer.get('result')

B58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
def b58decode(value):
    n=0
    for char in value: n=n*58+B58_ALPHABET.index(char)
    raw=n.to_bytes((n.bit_length()+7)//8,'big') if n else b''
    return b'\0'*(len(value)-len(value.lstrip('1')))+raw

def extract(sig,tx):
    if not tx or tx.get('meta',{}).get('err') is not None: return None
    msg=tx.get('transaction',{}).get('message',{})
    keys=msg.get('accountKeys',[])
    accounts=[k.get('pubkey') if isinstance(k,dict) else k for k in keys]
    instructions=list(msg.get('instructions',[]))
    for group in tx.get('meta',{}).get('innerInstructions',[]) or []:
        instructions.extend(group.get('instructions',[]))
    matched=[]; program_instructions=0; discriminators=[]
    for ins in instructions:
        program_id=ins.get('programId')
        if not program_id and isinstance(ins.get('programIdIndex'),int):
            idx=ins['programIdIndex']
            program_id=accounts[idx] if idx<len(accounts) else None
        if program_id!=PROGRAM: continue
        program_instructions+=1
        parsed=ins.get('parsed',{})
        typ=str(parsed.get('type','')).lower() if isinstance(parsed,dict) else ''
        if typ in ('initialize2','initialize_2'): matched.append('parsed_initialize2')
        data=ins.get('data')
        if isinstance(data,str):
            try:
                raw=b58decode(data)
                if raw:
                    discriminators.append(raw[0])
                    # Raydium AMM v4 LiquidityInstruction::Initialize2 = 1
                    if raw[0]==1: matched.append('amm_v4_discriminator_1')
            except ValueError: pass
    balances=tx.get('meta',{}).get('postTokenBalances',[]) or []
    mints=sorted({b.get('mint') for b in balances if b.get('mint')})
    return {'signature':sig,'slot':tx.get('slot'),'block_time':tx.get('blockTime'),
            'instruction_verified':bool(matched),'instruction_types':matched,
            'raydium_instruction_count':program_instructions,'discriminators':discriminators[:12],
            'token_mints_observed':mints,'account_count':len(accounts),
            'classification':'pool_init_instruction_verified' if matched else 'sampled_non_init_transaction'}

def heartbeat():
    while True:
        time.sleep(60)
        with LOCK:
            snapshot = {k:STATE[k] for k in ('connected','events','failed_transactions','filtered','init_markers','sampled','candidates','decoded','verified_inits','non_init_decoded','rpc_errors','rpc_null','dropped')}
        emit('heartbeat',**snapshot)

def decoder():
    spacing=60/MAX_RPM; next_call=0
    while True:
        sig, marker=QUEUE.get()
        try:
            remaining=next_call-time.monotonic()
            if remaining>0:time.sleep(remaining)
            next_call=time.monotonic()+spacing
            tx=rpc(sig)
            record=extract(sig,tx)
            if record:
                with LOCK:
                    STATE['decoded']+=1
                    if record['instruction_verified']:
                        STATE['verified_inits']+=1; STATE['last_candidate']=record
                        CANDIDATES.appendleft(record)
                    else: STATE['non_init_decoded']+=1
                emit('transaction_decoded',**record)
            else:
                bump('rpc_null')
                emit('candidate_unavailable',signature=sig)
        except (urllib.error.HTTPError,urllib.error.URLError,TimeoutError,RuntimeError,ValueError) as exc:
            bump('rpc_errors'); emit('rpc_error',signature=sig,error=str(exc)[:180])
            if isinstance(exc,urllib.error.HTTPError) and exc.code==429:
                next_call=time.monotonic()+max(30,spacing*3)
        except Exception as exc:
            bump('rpc_errors'); emit('decode_error',signature=sig,error=str(exc)[:180])
        finally:QUEUE.task_done()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlsplit(self.path).path.rstrip('/') or '/'
        with LOCK:
            data = dict(STATE)
            data['queue_depth'] = QUEUE.qsize()
            if path == '/candidates':
                data = {'version':'v0.7','mode':'research_only','candidates':list(CANDIDATES)}
            else:
                data['version'] = 'v0.7'
        if path not in ('/','/health','/candidates'):
            data = {'version':'v0.7','error':'not_found','requested_path':path,
                    'available_paths':['/','/health','/candidates']}
            code = 404
        else:
            code = 200
        raw=json.dumps(data,default=str).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type','application/json; charset=utf-8')
        self.send_header('X-Scanner-Version','v0.7')
        self.send_header('Cache-Control','no-store')
        self.send_header('Content-Length',str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)
        emit('http_request',path=path,status=code,version='v0.7')
    def log_message(self,*args): pass

if __name__=='__main__':
    emit('starting',version='v0.7',program=PROGRAM,rpc_max_rpm=MAX_RPM,sample_every=SAMPLE_EVERY,mode='research_only',port=int(os.getenv('PORT','10000')))
    threading.Thread(target=stream,daemon=True).start()
    threading.Thread(target=decoder,daemon=True).start()
    threading.Thread(target=heartbeat,daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),Handler).serve_forever()
