import os,time,json,sqlite3,urllib.request,datetime
DB=os.getenv('DB_PATH','scanner.db')
INTERVAL=max(60,int(os.getenv('POLL_SECONDS','120')))
MIN_LIQ=float(os.getenv('MIN_LIQ_USD','50000'))
MIN_VOL=float(os.getenv('MIN_VOL_H1_USD','20000'))
URL='https://api.dexscreener.com/token-profiles/latest/v1'

def fetch(url):
    req=urllib.request.Request(url,headers={'User-Agent':'SolanaResearchScanner/0.1'})
    with urllib.request.urlopen(req,timeout=20) as r:return json.load(r)

def init(db):
    db.execute('CREATE TABLE IF NOT EXISTS observations(ts TEXT, token TEXT, symbol TEXT, pair TEXT, price REAL, liquidity REAL, volume_h1 REAL, change_h1 REAL, PRIMARY KEY(ts,pair))')
    db.commit()

def scan(db):
    profiles=fetch(URL)
    addresses=list(dict.fromkeys(p.get('tokenAddress') for p in profiles if p.get('chainId')=='solana' and p.get('tokenAddress')))[:25]
    now=datetime.datetime.now(datetime.timezone.utc).isoformat()
    hits=[]
    for address in addresses:
        try:
            data=fetch('https://api.dexscreener.com/token-pairs/v1/solana/'+address)
            if not isinstance(data,list):continue
            for p in data:
                liq=float((p.get('liquidity') or {}).get('usd') or 0)
                vol=float((p.get('volume') or {}).get('h1') or 0)
                if liq<MIN_LIQ or vol<MIN_VOL:continue
                price=float(p.get('priceUsd') or 0)
                if price<=0:continue
                change=float((p.get('priceChange') or {}).get('h1') or 0)
                symbol=(p.get('baseToken') or {}).get('symbol','?')
                pair=p.get('pairAddress','')
                db.execute('INSERT OR IGNORE INTO observations VALUES (?,?,?,?,?,?,?,?)',(now,address,symbol,pair,price,liq,vol,change))
                hits.append({'token':symbol,'pair':pair,'price_usd':price,'liquidity_usd':liq,'volume_h1_usd':vol,'change_h1_pct':change})
        except Exception as e:print('pair_error',address,str(e),flush=True)
    db.commit()
    print(json.dumps({'time':now,'qualified_observations':len(hits),'results':hits},ensure_ascii=False),flush=True)

if __name__=='__main__':
    db=sqlite3.connect(DB);init(db)
    print('RESEARCH ONLY. No real orders. No demonstrated profitability.',flush=True)
    while True:
        try:scan(db)
        except Exception as e:print('scan_error',str(e),flush=True)
        time.sleep(INTERVAL)
