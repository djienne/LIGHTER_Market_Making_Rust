"""Independent account-state monitor for the Rust lighter-mm bot.

Polls the EXCHANGE directly (Python SDK + REST) — fully independent of the Rust bot's own
view — and prints position / open-order count / collateral. Enforces the hard invariant that
there are never more than MAX_ORDERS resting orders: any violation prints a loud ALERT and is
counted. Tracks the max order-count seen for the end-of-run summary.

Usage: python3 scripts_monitor.py [interval_sec] [max_orders]
"""
import os, sys, time, asyncio
sys.path.insert(0, '/home/ubuntu/lighter_MM')
from dotenv import load_dotenv
load_dotenv('/home/ubuntu/lighter_MM/.env')
import lighter, requests
URL='https://mainnet.zklighter.elliot.ai'
ACCT=int(os.environ['ACCOUNT_INDEX']); AKI=int(os.environ['API_KEY_INDEX']); PRIV=os.environ['API_KEY_PRIVATE_KEY']
MARKET=1
MAX_ORDERS=int(sys.argv[2]) if len(sys.argv)>2 else 4

async def snap(sc, acc_api):
    pos=0.0; coll='?'
    try:
        r=await acc_api.account(by='index', value=str(ACCT)); a=r.accounts[0]
        coll=getattr(a,'available_balance','?')
        for p in (a.positions or []):
            if int(p.market_id)==MARKET:
                s=float(p.position or 0); sign=getattr(p,'sign',1); pos=-s if int(sign)==-1 else s
    except Exception as e: coll=f'errAcc:{e}'
    n='?'
    try:
        tok,_=sc.create_auth_token_with_expiry()
        resp=requests.get(URL+'/api/v1/accountActiveOrders', params={'account_index':ACCT,'market_id':MARKET,'auth':tok}, timeout=10)
        n=len(resp.json().get('orders',[]))
    except Exception as e: n=f'errOrд:{e}'
    return pos, n, coll

async def main():
    interval=int(sys.argv[1]) if len(sys.argv)>1 else 30
    api=lighter.ApiClient(configuration=lighter.Configuration(host=URL))
    acc_api=lighter.AccountApi(api)
    sc=lighter.SignerClient(url=URL, account_index=ACCT, api_private_keys={AKI:PRIV})
    max_seen=0; violations=0
    print(f"MON start: interval={interval}s max_orders={MAX_ORDERS} acct={ACCT} aki={AKI} market={MARKET}", flush=True)
    while True:
        pos,n,coll=await snap(sc, acc_api)
        alert=''
        if isinstance(n,int):
            max_seen=max(max_seen,n)
            if n>MAX_ORDERS:
                violations+=1
                alert=f'  <<< ALERT orders={n} > MAX {MAX_ORDERS} (violation #{violations})'
        print(f"{time.strftime('%H:%M:%S')} MON pos={pos} orders={n} coll={coll} max_seen={max_seen}{alert}", flush=True)
        await asyncio.sleep(interval)

asyncio.run(main())
