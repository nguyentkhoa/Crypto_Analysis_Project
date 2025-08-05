import requests
list_coin = ['BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT','ADAUSDT','AVAXUSDT','DOTUSDT','LINKUSDT','TRXUSDT']
def get_all_spot_pairs(): # Lấy tên của các coin
    url = "https://api.bitget.com/api/v2/spot/market/tickers"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    products = data.get("data", [])
    return [item["symbol"] for item in products if item['symbol'] in list_coin ]
target_list_coin=get_all_spot_pairs()
target_list_coin
print('Sucessful')
