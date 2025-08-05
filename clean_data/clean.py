# Libraries
import pandas as pd
import pyodbc
import numpy as np
from load_to_sql import load_to_db
import pyodbc
# Function
def connection_string(server,database,username,password): # tạo kết nối 
    return f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

def import_data_from_DB(query,connection): # select data từ DB
    return pd.read_sql_query(f'{query}',connection)

def extract_coin_base_quote(df, col='instId'): # tách symbol
    pattern = r'([a-z]+)(usdt|btc|eth|bnb|sol|xrp|ada|avax|dot|link|trx)$'
    df[['coin_base', 'coin_quote']] = df[col].str.lower().str.extract(pattern)
    return df

coin_fullname_map = {
    'btc': 'Bitcoin',
    'eth': 'Ethereum',
    'bnb': 'Binance Coin',
    'sol': 'Solana',
    'xrp': 'Ripple',
    'ada': 'Cardano',
    'avax': 'Avalanche',
    'dot': 'Polkadot',
    'link': 'Chainlink',
    'trx': 'TRON',
}
def map_name_coin(df,col): # thêm cột fullname 
    df['coin_fullname']=df[col].map(coin_fullname_map)
    return df

def lower_all_df(df): # In thường các cột chuỗi
    list_string_col_trade=df.select_dtypes(include='object').columns
    for col in list_string_col_trade:
        df[col]=df[col].str.lower()
    return df 

def convert_timestamp_HCM(df, timestamp_col): # Chuyển ts của websock về ts của VietNam
    df[timestamp_col] = (
        pd.to_datetime(df[timestamp_col], unit='ms', utc=True)
          .dt.tz_convert('Asia/Ho_Chi_Minh')
          .dt.tz_localize(None)
          .dt.floor('min')
    )
    return df

def convert_date(df,timestamp_col): # Chuyển dtype thành date
    df[timestamp_col]=pd.to_datetime(df[timestamp_col])
    return df

def remove_dup_value(df,col): # Xóa trùng lặp
    if isinstance(col,list):
        df.drop_duplicates(subset=col,inplace=True)
    else:
        df.drop_duplicates(subset=[col],inplace=True)
    return df

def rename_columns(df,columns_map):
    return df.rename(columns=columns_map)

def add_datetimekey_column(df, datetime_col, key_col='datetimekey'):
    df[key_col] = df[datetime_col].dt.strftime('%Y%m%d%H%M')
    return df

# Hàm xử lý bảng trade
def process_trade_pipeline(df):
    trade_old_columns=list(df.columns)
    trade_new_columns=['timestamp', 'price', 'size', 'side', 'trade_id', 'symbol', 'channel', 'type', 'timestamp_received']
    trade_columns_map=dict(zip(trade_old_columns,trade_new_columns))
    list_col_trade=['trade_id','datetimekey','symbol','coin_base','coin_quote','coin_fullname'
                ,'channel','type','timestamp','price','size','side']
    return (df
                  .pipe(extract_coin_base_quote,col='instId')
                  .pipe(map_name_coin,col='coin_base')
                  .pipe(lower_all_df)
                  .pipe(convert_timestamp_HCM,'ts')
                  .pipe(convert_date,'ts_recv')
                  .pipe(remove_dup_value,'tradeId')
                  .pipe(rename_columns,trade_columns_map)
                  .assign(symbol = lambda d:d['coin_base']+d['coin_quote'])
                  .pipe(add_datetimekey_column,'timestamp')
                  )[list_col_trade]

# Hàm xử lý bảng ticker
def process_ticker_pipeline(df):
    ticker_old_columns=list(df.columns) 
    ticker_new_columns=['symbol','last_price','open_24h','high_24h','low_24h','change_24h','bid_price','ask_price','bid_size','ask_size'
                    ,'base_volume','quote_volume','open_utc','change_utc_24h','timestamp','channel','type','timestamp_received']
    ticker_columns_map=dict(zip(ticker_old_columns,ticker_new_columns))
    list_col_ticker=['symbol','datetimekey','coin_base','coin_quote','coin_fullname','channel','type','timestamp'
                ,'last_price','open_24h','high_24h','low_24h','change_24h'
                ,'bid_price','ask_price','bid_size','ask_size','base_volume'
                ,'quote_volume','open_utc','change_utc_24h']
    
    return  (df
                .pipe(extract_coin_base_quote,col='instId')
                .pipe(map_name_coin,col='coin_base')
                .pipe(lower_all_df)
                .pipe(convert_timestamp_HCM,'ts')
                .pipe(convert_date,'ts_recv')
                .pipe(remove_dup_value,['instId','ts'])
                .pipe(rename_columns,ticker_columns_map)
                .pipe(add_datetimekey_column,'timestamp')
            )[list_col_ticker]

# Hàm xử lý bảng candle
def process_candle_pipeline(df):
    candle_old_columns=list(df.columns)
    candle_new_columns=['symbol', 'channel', 'type', 'timestamp_received', 'timestamp', 'open_price', 'highest_price', 
                        'lowest_price', 'closing_price', 'trading_volume_coin', 'trading_volume_usd']
    candle_columns_map=dict(zip(candle_old_columns,candle_new_columns))
    list_col_candle=['symbol','datetimekey','coin_base','coin_quote','coin_fullname','channel','type','timestamp','open_price'
                    ,'highest_price','lowest_price','closing_price','trading_volume_coin','trading_volume_usd']
    return   ( df
                .pipe(extract_coin_base_quote,col='instId')
                .pipe(map_name_coin,col='coin_base')
                .pipe(lower_all_df)
                .pipe(convert_timestamp_HCM,'start_time')
                .pipe(convert_date,'ts_recv')
                .pipe(remove_dup_value,['instId','start_time'])
                .pipe(rename_columns,candle_columns_map)
                .pipe(add_datetimekey_column,'timestamp')
            )[list_col_candle]

# Update is_processed to 1 for processed rows in the database
def update_is_processed(table, connection):
    cursor = connection.cursor()
    query = f"UPDATE {table} SET is_processed = 1 WHERE is_processed = 0"
    cursor.execute(query)
    connection.commit()
    cursor.close()

# Example usage for LINKUSDT (variable a)
def main():
    # Connection string
    connection = pyodbc.connect(connection_string(server='localhost',database='Coin_Analysis_DB',username='sa',password='2405'))
    # Import data
    raw_data_ticker=import_data_from_DB('SELECT * FROM [raw].[ticker_data] WHERE is_processed = 0',connection)
    raw_data_trade=import_data_from_DB('SELECT * FROM [raw].[trade_data] WHERE is_processed = 0',connection)
    raw_data_candle=import_data_from_DB('SELECT * FROM [raw].[candle_data] WHERE is_processed = 0',connection)
    # Copy data 
    copy_data_ticker=raw_data_ticker.copy()
    copy_data_trade=raw_data_trade.copy()
    copy_data_candle=raw_data_candle.copy()
    # Clean Data
    clean_data_ticker=process_ticker_pipeline(copy_data_ticker)
    clean_data_trade=process_trade_pipeline(copy_data_trade)
    clean_data_candle=process_candle_pipeline(copy_data_candle)
    # Load to clean schema
    load_to_db('clean','ticker_data',clean_data_ticker)
    load_to_db('clean','trade_data',clean_data_trade)
    load_to_db('clean','candle_data',clean_data_candle)
    # Update table raw
    update_is_processed('raw.ticker_data',connection)
    update_is_processed('raw.trade_data',connection)
    update_is_processed('raw.candle_data',connection)
    # Close Connection
    connection.close()
if __name__ == "__main__":
    main()
    print("Data processing completed successfully.")