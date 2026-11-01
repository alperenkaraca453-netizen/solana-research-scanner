"""Read-only, best-effort analytics. Never sends transactions or makes trading decisions."""
import collections, datetime, json, os, threading, time, urllib.request
from urllib.parse import quote

LOCK=threading.RLock()
TRACKED={}
STATS={'tracked':0,'price_samples':0,'price_errors':0,'safety_checks':0,'safety_errors':0}
POLL_SECONDS=max(30,int(os.getenv('PRICE_POLL_SECONDS','60')))
FEE_BPS=float(os.getenv('PAPER_TOTAL_COST_BPS','100'))  # assumed round trip, includes slippage/fees
MAX_TRACKED=max(1,min(100,int(os.getenv('MAX_TRACKED_POOLS','30'))))
STORE=os.getenv('ANALYTICS_JSONL','/tmp/solana_analytics.jsonl')
WSOL='So11111111111111111111111111111111111111112'
STABLE={'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v','Es9vMFrzaCERZzLCgA9hVN6Z1kY5T4h3Y8JYBfXQ5'}

def fetch(url,timeout=12):
    req=urllib.request.Request(url,headers={'User-Agent':'solana-research-scanner/1.1','Accept':'application/json'})
    with urllib.request.urlopen(req,timeout=timeout) as response:return json.load(response)

def record(event,**data):
    try:
        with open(STORE,'a',encoding='utf-8') as f:f.write(json.dumps({'ts':datetime.datetime.now(datetime.timezone.utc).isoformat(),'event':event,**data},default=str)+'\n')
    except OSError:pass

def add_pool(candidate):
    if not candidate.get('instruction_verified'):return
    signature=candidate['signature']
    pair=candidate.get('token_pair_mints') or []
    accounts=candidate.get('instruction_accounts') or []
    # Raydium CPMM Initialize: pool state is index 3. Other programs remain untracked.
    pool_address=accounts[3] if candidate.get('program')=='cpmm' and len(accounts)>3 else None
    with LOCK:
        if signature in TRACKED:return
        if len(TRACKED)>=MAX_TRACKED:
            oldest=min(TRACKED,key=lambda s:TRACKED[s]['detected_at_epoch']);TRACKED.pop(oldest)
        TRACKED[signature]={'signature':signature,'program':candidate.get('program'),'pool_address':pool_address,
            'token_pair_mints':pair,'detected_at_epoch':time.time(),'detected_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'status':'awaiting_price','samples':[],'milestones':{},'safety':{'status':'not_checked'},'last_error':None}
        STATS['tracked']+=1
    record('pool_tracked',signature=signature,pool_address=pool_address,token_pair_mints=pair)

def sample_one(sig):
    with LOCK:
        item=TRACKED.get(sig)
        if not item:return
        pair=item['token_pair_mints'];address=item['pool_address']
    if not address or len(pair)!=2:
        with LOCK:item['status']='unsupported_pool_address_or_pair'
        return
    # Token-pairs endpoint returns all pools containing the mint. Require exact pool address match.
    token=next((x for x in pair if x not in STABLE and x!=WSOL),pair[0])
    response=fetch('https://api.dexscreener.com/token-pairs/v1/solana/'+quote(token,safe=''))
    if not isinstance(response,list):raise ValueError('Unexpected market response')
    matches=[p for p in response if str(p.get('pairAddress',''))==address and p.get('chainId')=='solana']
    if not matches:
        with LOCK:item['status']='pool_not_indexed';item['last_error']=None
        return
    p=matches[0]
    price=float(p.get('priceUsd') or 0)
    liquidity=p.get('liquidity') or {}
    usd=liquidity.get('usd')
    base=(p.get('baseToken') or {}).get('address')
    if price<=0 or not base:raise ValueError('Missing valid base price')
    snap={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'epoch':time.time(),'price_usd':price,
          'priced_token_mint':base,'liquidity_usd':float(usd) if usd is not None else None,
          'dex_url':p.get('url'),'volume_h24_usd':(p.get('volume') or {}).get('h24')}
    with LOCK:
        item=TRACKED.get(sig)
        if not item:return
        if item['samples'] and item['samples'][0]['priced_token_mint']!=base:
            item['status']='price_token_changed';return
        item['samples'].append(snap)
        if len(item['samples'])>150:item['samples']=item['samples'][-150:]
        item['status']='tracking';item['last_error']=None;STATS['price_samples']+=1
        baseline=item['samples'][0]
        for minutes in (5,15,60):
            key=str(minutes)+'m'
            if key not in item['milestones'] and snap['epoch']-baseline['epoch']>=minutes*60:
                gross=(price/baseline['price_usd']-1)*100
                item['milestones'][key]={'elapsed_seconds':round(snap['epoch']-baseline['epoch']),
                    'gross_change_pct':round(gross,4),'illustrative_net_pct':round(gross-FEE_BPS/100,4),
                    'assumed_round_trip_cost_bps':FEE_BPS,'basis':'first_observed_market_price_not_pool_creation_price',
                    'entry_at':baseline['at'],'exit_at':snap['at']}
    record('price_sample',signature=sig,**snap)

def worker():
    while True:
        with LOCK:ids=list(TRACKED)
        for sig in ids:
            try:sample_one(sig)
            except Exception as exc:
                with LOCK:
                    STATS['price_errors']+=1
                    if sig in TRACKED:TRACKED[sig]['last_error']=str(exc)[:180]
                record('price_error',signature=sig,error=str(exc)[:180])
            time.sleep(1)
        time.sleep(POLL_SECONDS)

def snapshot():
    with LOCK:
        pools=[]
        for x in TRACKED.values():
            y=dict(x);y['samples_count']=len(x['samples']);y['first_sample']=x['samples'][0] if x['samples'] else None
            y['latest_sample']=x['samples'][-1] if x['samples'] else None
            y.pop('samples');pools.append(y)
        return {'version':'v1.1','mode':'research_only','trading_enabled':False,
            'assumed_round_trip_cost_bps':FEE_BPS,'price_source':'DexScreener (third-party, delayed/indexing possible)',
            'storage':'ephemeral local JSONL; resets on redeploy or Render sleep/restart',
            'pnl_warning':'Illustrative price change minus assumed fixed costs, not executable PnL; no actual fills, orderbook, tax, MEV, or realized slippage',
            'stats':dict(STATS),'pools':pools}
