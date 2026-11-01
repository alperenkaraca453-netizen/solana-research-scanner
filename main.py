"""Raydium AMM v4 + CPMM pool creation monitor. Research only, never trades."""
import collections, csv, datetime, hashlib, io, json, os, queue, re, threading, time, urllib.request, urllib.error
from urllib.parse import urlsplit
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from websocket import create_connection

PROGRAMS = {'amm_v4':'675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            'cpmm':'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C'}
WS_URL=os.getenv('SOLANA_WS_URL','wss://api.mainnet-beta.solana.com/')
HTTP_URL=os.getenv('SOLANA_HTTP_URL','https://api.mainnet-beta.solana.com')
MAX_RPM=max(1,min(60,int(os.getenv('RPC_MAX_RPM','12'))))
MAX_TX_VERSION=max(1,int(os.getenv('MAX_SUPPORTED_TX_VERSION','1')))
SAMPLE_EVERY=max(0,int(os.getenv('SAMPLE_EVERY','0')))  # default: no wasteful sampling
QUEUE=queue.PriorityQueue(maxsize=500)
LOCK=threading.RLock()
STATE=dict(connected=False,connections={},last_event=None,last_error=None,events=0,
           events_by_program={'amm_v4':0,'cpmm':0},failed_transactions=0,
           init_markers=0,candidates=0,verified_inits=0,decoded=0,non_init_decoded=0,
           sampled=0,rpc_errors=0,version_errors=0,rpc_null=0,dropped=0,
           duplicates=0,queue_depth=0,last_candidate=None,mode='research_only',live_trading=False)
CANDIDATES=collections.deque(maxlen=500)
STARTED_AT=datetime.datetime.now(datetime.timezone.utc).isoformat()
START_MONO=time.monotonic()
SEEN=collections.deque(maxlen=5000)
SEEN_SET=set()
SEQ=0
B58='123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
CPMM_INIT=hashlib.sha256(b'global:initialize').digest()[:8]
AMM_RE=re.compile(r'initialize[_ ]?2|initializeinstruction2',re.I)
CPMM_RE=re.compile(r'Instruction:\s*Initialize\b',re.I)

def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def emit(event,**kw):print(json.dumps({'ts':now(),'event':event,**kw},default=str),flush=True)
def bump(k):
    with LOCK:STATE[k]+=1

def b58decode(s):
    n=0
    for c in s:n=n*58+B58.index(c)
    return b'\0'*(len(s)-len(s.lstrip('1')))+(n.to_bytes((n.bit_length()+7)//8,'big') if n else b'')

def enqueue(value,kind):
    global SEQ
    sig=value.get('signature')
    if not sig:return
    if value.get('err') is not None:bump('failed_transactions');return
    logs=value.get('logs') or []
    marker=any((AMM_RE if kind=='amm_v4' else CPMM_RE).search(str(line)) for line in logs)
    with LOCK:
        if marker:STATE['init_markers']+=1
        # Samples are diagnostic only and never considered pool candidates.
        sample=bool(SAMPLE_EVERY and STATE['events_by_program'][kind]%SAMPLE_EVERY==0)
        if not(marker or sample):return
        if sig in SEEN_SET:STATE['duplicates']+=1;return
        if len(SEEN)==SEEN.maxlen:SEEN_SET.discard(SEEN.popleft())
        SEEN.append(sig);SEEN_SET.add(sig)
        if sample and not marker:STATE['sampled']+=1
        SEQ+=1;seq=SEQ
    try:
        QUEUE.put_nowait((0 if marker else 1,seq,sig,kind,marker))
        if marker:bump('candidates')
        emit('transaction_queued',signature=sig,program=kind,reason='init_marker' if marker else 'sample')
    except queue.Full:bump('dropped');emit('queue_full',program=kind)

def stream(kind,program):
    delay=2
    while True:
        ws=None
        try:
            ws=create_connection(WS_URL,timeout=25);ws.settimeout(50)
            ws.send(json.dumps({'jsonrpc':'2.0','id':1,'method':'logsSubscribe','params':[{'mentions':[program]},{'commitment':'confirmed'}]}))
            ack=json.loads(ws.recv())
            if not isinstance(ack.get('result'),int):raise RuntimeError('subscription rejected '+str(ack)[:180])
            with LOCK:
                STATE['connections'][kind]=True;STATE['connected']=all(STATE['connections'].get(k,False) for k in PROGRAMS)
                STATE['last_error']=None
            emit('subscribed',program=kind,program_id=program,subscription=ack['result']);delay=2
            while True:
                msg=json.loads(ws.recv())
                if msg.get('method')!='logsNotification':continue
                val=msg.get('params',{}).get('result',{}).get('value',{})
                with LOCK:
                    STATE['events']+=1;STATE['events_by_program'][kind]+=1;STATE['last_event']=now()
                enqueue(val,kind)
        except Exception as exc:
            with LOCK:
                STATE['connections'][kind]=False;STATE['connected']=False;STATE['last_error']=f'{kind}: {exc}'
            emit('stream_error',program=kind,error=str(exc));time.sleep(delay);delay=min(60,delay*2)
        finally:
            if ws:
                try:ws.close()
                except Exception:pass

def rpc(sig):
    payload=json.dumps({'jsonrpc':'2.0','id':1,'method':'getTransaction','params':[sig,{'encoding':'jsonParsed','commitment':'confirmed','maxSupportedTransactionVersion':MAX_TX_VERSION}]}).encode()
    req=urllib.request.Request(HTTP_URL,data=payload,headers={'Content-Type':'application/json','User-Agent':'solana-research-v09'})
    with urllib.request.urlopen(req,timeout=18) as res:answer=json.load(res)
    if 'error' in answer:raise RuntimeError(str(answer['error'])[:250])
    return answer.get('result')

def extract(sig,tx,kind):
    if not tx or tx.get('meta',{}).get('err') is not None:return None
    msg=tx.get('transaction',{}).get('message',{})
    accounts=[x.get('pubkey') if isinstance(x,dict) else x for x in msg.get('accountKeys',[])]
    instructions=list(msg.get('instructions',[]))
    for group in tx.get('meta',{}).get('innerInstructions',[]) or []:instructions.extend(group.get('instructions',[]))
    matches=[];discriminators=[];program_instruction_count=0;pool_accounts=[]
    program=PROGRAMS[kind]
    for ins in instructions:
        pid=ins.get('programId')
        if not pid and isinstance(ins.get('programIdIndex'),int):
            i=ins['programIdIndex'];pid=accounts[i] if i<len(accounts) else None
        if pid!=program:continue
        program_instruction_count+=1
        parsed=ins.get('parsed')
        typ=str(parsed.get('type','')).lower() if isinstance(parsed,dict) else ''
        data=ins.get('data')
        raw=b''
        if isinstance(data,str):
            try:raw=b58decode(data)
            except ValueError:pass
        if kind=='amm_v4':
            if raw:discriminators.append(raw[0])
            verified=(bool(raw) and raw[0]==1) or typ in ('initialize2','initialize_2')
        else:
            if raw:discriminators.append(raw[:8].hex())
            verified=(len(raw)>=8 and raw[:8]==CPMM_INIT) or typ=='initialize'
        if verified:
            matches.append(f'{kind}_initialize')
            ins_accounts=ins.get('accounts') or []
            pool_accounts=[accounts[a] if isinstance(a,int) and a<len(accounts) else a for a in ins_accounts]
    mints=sorted({b.get('mint') for b in (tx.get('meta',{}).get('postTokenBalances') or []) if b.get('mint')})
    # CPMM initialize instruction account indices 4/5/6 = pool authority / token0 mint / token1 mint.
    # For AMM v4, leave pair unknown rather than guessing from transaction-wide balances.
    pair=pool_accounts[5:7] if kind=='cpmm' and len(pool_accounts)>=7 and matches else []
    return {'signature':sig,'program':kind,'slot':tx.get('slot'),'block_time':tx.get('blockTime'),
            'token_pair_mints':pair,'liquidity_usd':None,'risk_score':None,'paper_pnl_usd':None,
            'instruction_verified':bool(matches),'instruction_types':matches,
            'program_instruction_count':program_instruction_count,'discriminators':discriminators[:10],
            'token_mints_observed':mints,'instruction_accounts':pool_accounts[:30],
            'classification':'pool_init_instruction_verified' if matches else 'non_init_transaction'}

def decoder():
    next_call=0;spacing=60/MAX_RPM
    while True:
        priority,seq,sig,kind,marker=QUEUE.get()
        try:
            wait=next_call-time.monotonic()
            if wait>0:time.sleep(wait)
            next_call=time.monotonic()+spacing
            tx=rpc(sig);record=extract(sig,tx,kind)
            if record:
                with LOCK:
                    STATE['decoded']+=1
                    if record['instruction_verified']:
                        STATE['verified_inits']+=1;STATE['last_candidate']=record;CANDIDATES.appendleft(record)
                    else:STATE['non_init_decoded']+=1
                emit('transaction_decoded',**record)
            else:bump('rpc_null');emit('transaction_unavailable',signature=sig,program=kind)
        except Exception as exc:
            bump('rpc_errors')
            if '-32015' in str(exc):bump('version_errors')
            emit('rpc_error',signature=sig,program=kind,error=str(exc)[:200])
            if isinstance(exc,urllib.error.HTTPError) and exc.code==429:next_call=time.monotonic()+max(30,spacing*3)
        finally:QUEUE.task_done()

def heartbeat():
    while True:
        time.sleep(60)
        with LOCK:state=dict(STATE)
        state['queue_depth']=QUEUE.qsize()
        emit('heartbeat',**state)

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path=urlsplit(self.path).path.rstrip('/') or '/'
        with LOCK:
            data=dict(STATE)
            data['connections']=dict(STATE['connections']);data['events_by_program']=dict(STATE['events_by_program'])
            data['queue_depth']=QUEUE.qsize()
            data['started_at']=STARTED_AT
            data['uptime_seconds']=round(time.monotonic()-START_MONO)
            data['last_event_age_seconds']=round((datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat(STATE['last_event'])).total_seconds()) if STATE['last_event'] else None
            if path=='/candidates':data={'version':'v1.0','mode':'research_only','candidates':list(CANDIDATES)}
            elif path=='/report':data={'version':'v1.0','generated_at':now(),'started_at':STARTED_AT,'uptime_seconds':round(time.monotonic()-START_MONO),'events':STATE['events'],'verified_pools':STATE['verified_inits'],'rpc_errors':STATE['rpc_errors'],'dropped':STATE['dropped'],'queue_depth':QUEUE.qsize(),'connections':dict(STATE['connections']),'last_event_age_seconds':data['last_event_age_seconds'],'paper_trades':0,'realized_pnl_usd':None,'note':'No price/liquidity source connected; profit cannot be calculated yet.'}
            elif path=='/':data={'version':'v1.0','mode':'research_only','status':data,'endpoints':['/health','/candidates','/report','/export.csv']}
            else:data['version']='v1.0'
        if path=='/export.csv':
            buf=io.StringIO();fields=['signature','program','slot','block_time','token_pair_mints','instruction_verified','liquidity_usd','risk_score','paper_pnl_usd']
            writer=csv.DictWriter(buf,fieldnames=fields,extrasaction='ignore');writer.writeheader()
            for item in list(CANDIDATES):
                row=dict(item);row['token_pair_mints']=' | '.join(row.get('token_pair_mints',[]));writer.writerow(row)
            raw=buf.getvalue().encode('utf-8');self.send_response(200);self.send_header('Content-Type','text/csv; charset=utf-8');self.send_header('Content-Disposition','attachment; filename=verified_pools.csv');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw);return
        code=200 if path in ('/','/health','/candidates','/report') else 404
        if code==404:data={'error':'not_found','endpoints':['/','/health','/candidates']}
        raw=json.dumps(data,default=str).encode()
        self.send_response(code);self.send_header('Content-Type','application/json; charset=utf-8')
        self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)))
        self.end_headers();self.wfile.write(raw)
    def log_message(self,*args):pass

if __name__=='__main__':
    emit('starting',version='v1.0',programs=PROGRAMS,rpc_max_rpm=MAX_RPM,sample_every=SAMPLE_EVERY,mode='research_only')
    for kind,program in PROGRAMS.items():threading.Thread(target=stream,args=(kind,program),daemon=True).start()
    threading.Thread(target=decoder,daemon=True).start()
    threading.Thread(target=heartbeat,daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),Handler).serve_forever()
