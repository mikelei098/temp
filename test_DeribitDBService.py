import psycopg2
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt

''' Database Parameters '''

# HOST = 'localhost'
# PORT = '5432'
# USER = 'postgres'
# PASSWORD = ''
# DATABASE = {"deribit": "db_basis_trading"}



HOST = "database-1.cakb9ilpbyfg.us-east-1.rds.amazonaws.com"
PORT = '5432'
USER = 'postgres'
PASSWORD = 'postgres123'
DATABASE = {"deribit": "db_basis_trading"}

''' Creation and Initialization '''


################################################
def initialize(exchange, currency={'BTC': "Bitcoin",
                                   'ETH': "Ethereum",
                                   }):
    ### Create database
    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database='postgres'
    )

    con.autocommit = True
    # cursor
    cur = con.cursor()

    # Create database
    Query = '''
            CREATE DATABASE {};
            '''.format(DATABASE[exchange])
    cur.execute(Query)
    # cur.close()
    # con.commit()
    # con.close()

    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )

    # cursor
    cur = con.cursor()

    # CREATE TABLE currency
    cur.execute("""/* DROP TABLE currency; */
                CREATE TABLE IF NOT EXISTS currency (
                    ticker VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(50) UNIQUE NOT NULL
                );
                """)

    # Insert currencies
    for ticker, name in currency.items():
        cur.execute("""
                    INSERT INTO currency(ticker,name) VALUES(%s,%s)
                    ON CONFLICT DO NOTHING;
                    """, (ticker, name)
                    )

    # Create Table futures
    cur.execute("""
                    /* DROP TABLE futures; */
                    CREATE TABLE IF NOT EXISTS futures (
                        contract_code VARCHAR(50) PRIMARY KEY,
                        currency_ticker VARCHAR(50) NOT NULL,
                        expiration_timestamp BIGINT NOT NULL,
                        creation_timestamp BIGINT NOT NULL,
                        FOREIGN KEY (currency_ticker) REFERENCES currency (ticker)
                    );
                    """)

    # CREATE TABLE ts_index
    cur.execute("""
                    /* DROP TABLE ts_index; */
                    CREATE TABLE IF NOT EXISTS ts_index (
                        index_code VARCHAR(50) NOT NULL,
                        unix_timestamp BIGINT NOT NULL,  
                        occurred_time TIMESTAMP NOT NULL,
                        price FLOAT8 NOT NULL,
                        currency_ticker VARCHAR(50) NOT NULL,                     
                        PRIMARY KEY (index_code, unix_timestamp),
                        FOREIGN KEY (currency_ticker) REFERENCES currency (ticker)    
                    );
                    """
                )

    # CREATE TABLE trades_futures
    cur.execute("""
                /* DROP TABLE trades_futures; */
                CREATE TABLE IF NOT EXISTS trades_futures (
                    trade_id VARCHAR(50) NOT NULL,
                    contract_code VARCHAR(50) NOT NULL,
                    occurred_time TIMESTAMP NOT NULL,
                    price FLOAT8 NOT NULL,
                    volume FLOAT8 NOT NULL,
                    unix_timestamp BIGINT NOT NULL,                                     
                    PRIMARY KEY (trade_id),
                    FOREIGN KEY (contract_code) REFERENCES futures (contract_code)    
                );
                """
                )

    # close the cursor
    cur.close()
    con.commit()
    # close the connection
    con.close()


""" Insert Function """
#################################################
def insert_currency(exchange, Ticker, Name):
    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )

    # cursor
    cur = con.cursor()
    cur.execute("""
            INSERT INTO currency(ticker, name) VALUES(%s,%s)
            ON CONFLICT DO NOTHING;
            """, (Ticker, Name)
                )

    # close the cursor
    cur.close()
    con.commit()
    # close the connection
    con.close()

def insert_futures(exchange,
                   contract_code=None,
                   currency=None,
                   expiration_timestamp = None,
                   creation_timestamp = None,
                   df=pd.DataFrame()):
    """
    :param exchange:
    :param Contract_code:
    :param Currency:
    :param df:
    :return:
    """
    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )

    # cursor
    cur = con.cursor()
    if not df.empty:
        for idx in df.index:
            if 'contract_code' in df.columns:
                contract_code = df.loc[idx, 'contract_code']
            if 'currency' in df.columns:
                currency = df.loc[idx, 'currency']
            if 'expiration_timestamp' in df.columns:
                expiration_timestamp = int(df.loc[idx, 'expiration_timestamp'])
            if 'creation_timestamp' in df.columns:
                creation_timestamp = int(df.loc[idx, 'creation_timestamp'])

            cur.execute("""       
                    INSERT INTO futures(contract_code,
                                        currency_ticker,
                                        expiration_timestamp,
                                        creation_timestamp
                                        ) 
                        VALUES(%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING;
                    """, (contract_code,
                          currency,
                          expiration_timestamp,
                          creation_timestamp
                          )
                    )
    else:
        # print('insert futures contract: ', Contract_code, Currency)
        cur.execute("""       
                     INSERT INTO futures(contract_code,
                                         currency_ticker,
                                         expiration_timestamp,
                                         creation_timestamp
                                         ) 
                         VALUES(%s,%s,%s,%s)
                         ON CONFLICT DO NOTHING;
                     """, (contract_code,
                           currency,
                           expiration_timestamp,
                           creation_timestamp
                           )
                    )
    # close the cursor
    cur.close()
    con.commit()
    # close the connection
    con.close()


def insert_ts_index(exchange,
                    index_code=None,
                    occurred_time=None,
                    price=None,
                    unix_timestamp=None,
                    currency=None,
                    df=pd.DataFrame()):
    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )

    # cursor
    cur = con.cursor()
    if not df.empty:
        for idx in df.index:
            if 'index_code' in df.columns:
                index_code = df.loc[idx, 'index_code']
            if 'occurred_time' in df.columns:
                occurred_time = df.loc[idx, 'occurred_time'].strftime("%Y-%m-%d %H:%M:%S.%f")
            if 'price' in df.columns:
                price = float(df.loc[idx, 'price'])
            if 'unix_timestamp' in df.columns:
                unix_timestamp = int(df.loc[idx, 'unix_timestamp'])
            if 'currency' in df.columns:
                currency = df.loc[idx, 'currency'].upper()
            cur.execute("""
                        INSERT INTO ts_index(index_code, occurred_time, price, unix_timestamp, currency_ticker) 
                            VALUES( %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s
                                    )
                            ON CONFLICT DO NOTHING;
                        """, (index_code, occurred_time, price, unix_timestamp, currency)
                        )
    else:
        cur.execute("""
                    INSERT INTO ts_index(index_code, occurred_time, price, unix_timestamp, currency_ticker) 
                        VALUES( %s,
                                %s,
                                %s,
                                %s,
                                %s
                                )
                        ON CONFLICT DO NOTHING;
                    """, (index_code, occurred_time, price, unix_timestamp, currency,)
                    )

    # close the cursor
    cur.close()
    con.commit()
    # close the connection
    con.close()


def insert_trades_futures(exchange,
                          trade_id=None,
                          contract_code=None,
                          occurred_time=None,
                          price=None,
                          volume=None,
                          unix_timestamp=None,
                          df=pd.DataFrame()):
    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )
    # cursor
    cur = con.cursor()
    if not df.empty:
        for idx in df.index:
            if 'trade_id' in df.columns:
                trade_id = df.loc[idx, 'trade_id']
            if 'contract_code' in df.columns:
                contract_code = df.loc[idx, 'contract_code']
            if 'occurred_time' in df.columns:
                occurred_time = df.loc[idx, 'occurred_time'].strftime("%Y-%m-%d %H:%M:%S.%f")
            if 'price' in df.columns:
                price = float(df.loc[idx, 'price'])
            if 'volume' in df.columns:
                volume = float(df.loc[idx, 'volume'])
            if 'unix_timestamp' in df.columns:
                unix_timestamp = int(df.loc[idx, 'unix_timestamp'])
            cur.execute("""
                        INSERT INTO trades_futures(trade_id, contract_code, occurred_time, price, volume, unix_timestamp) 
                            VALUES(%s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s
                                    )                        
                            ON CONFLICT DO NOTHING;
                        """, (trade_id, contract_code, occurred_time, price, volume, unix_timestamp)
                        )

    else:
        cur.execute("""
                    INSERT INTO trades_futures(trade_id, contract_code, occurred_time, price, volume, unix_timestamp) 
                        VALUES(%s,
                                %s,
                                %s,
                                %s,
                                %s,
                                %s
                                )                        
                        ON CONFLICT DO NOTHING;
                    """, (trade_id, contract_code, occurred_time, price, volume, unix_timestamp)
                    )

    # close the cursor
    cur.close()
    con.commit()
    # close the connection
    con.close()



def update(exchange):
    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )

    # cursor
    cur = con.cursor()
    cur.execute("""
            UPDATE currency
            SET name = 'Bitcoin', ticker = 'BTC'
            WHERE id = 1

            """)
    # close the cursor
    cur.close()
    con.commit()
    # close the connection
    con.close()
    return


""" Query Functions """


#################################################
def query_currency(exchange):
    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )

    Query = """
    SELECT ticker, name from currency
    """

    res = pd.read_sql_query(Query, con)
    return res


def query_trades_futures(exchange, currency, start, end, freq = '1min'):
    # unify the format !! IMPORTANT !!
    if not isinstance(start, str):
        start = dt.datetime.strftime(start, '%Y-%m-%d %T')
    start = "\'" + start + "\'"
    if not isinstance(end, str):
        end = dt.datetime.strftime(end, '%Y-%m-%d %T')
    end = "\'" + end + "\'"
    currency = "\'" + currency.upper() + "\'"

    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )
    Query = '''
    SELECT tf.contract_code, tf.occurred_time, tf.price, tf.volume
    FROM trades_futures AS tf
    JOIN futures AS f
    ON f.contract_code = tf.contract_code
    WHERE tf.occurred_time >= {} AND tf.occurred_time <= {} AND f.currency_ticker = {} 
    ORDER BY tf.occurred_time
    '''.format(start, end, currency)

    df = pd.read_sql_query(Query, con)
    df.columns = ['contract_code', "time", "price", "volume"]
    con.close()

    df.set_index(keys='time', inplace=True)
    res = dict()
    for i in df.groupby('contract_code'):
        contract_code = i[0]
        tmp = i[1]
        price = tmp['price'].resample(freq).ohlc()
        volume = tmp['volume'].resample(freq).sum()
        values = pd.concat([price, volume], axis=1)
        res[contract_code] = values
        # tmp.dropna(axis=0, inplace=True)
        # print(tmp)
        # tmp.columns = tmp.columns.map(lambda x: x.capitalize())
    return res

def query_index_price(exchange, currency, start, end, freq = '1min'):
    # unify the format !! IMPORTANT !!
    if not isinstance(start, str):
        start = dt.datetime.strftime(start, '%Y-%m-%d %T')
    start = "\'" + start + "\'"
    if not isinstance(end, str):
        end = dt.datetime.strftime(end, '%Y-%m-%d %T')
    end = "\'" + end + "\'"
    currency = "\'" + currency.upper() + "\'"

    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )
    Query = '''
    SELECT occurred_time, price 
    FROM ts_index
    WHERE occurred_time >= {} AND occurred_time <= {} AND currency_ticker = {} 
    ORDER BY occurred_time
    '''.format(start, end, currency)

    df = pd.read_sql_query(Query, con)
    df.columns = ["time", "price"]
    con.close()

    df.set_index(keys='time', inplace=True)
    price = df['price'].resample(freq).ohlc()
    return price

def query_future_price(exchange, Contract_code, df):
    Contract_code = "\'" + Contract_code + "\'"
    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )
    cur = con.cursor()
    df['Spot'] = 0
    for idx in df.index:
        Occurred_time = df.Occurred_time[idx]
        # unify the format !! IMPORTANT !!
        if not isinstance(Occurred_time, str):
            Occurred_time = dt.datetime.strftime(Occurred_time, '%Y-%m-%d %T')
        Occurred_time = "\'" + Occurred_time + "\'"
        Query = '''
                SELECT tsf.price
                FROM ts_futures AS tsf
                JOIN futures AS f
                ON f.contract_code = tsf.contract_code
                WHERE DATE_TRUNC('DAY',tsf.occurred_time) = {}  AND f.contract_code = {} 
                '''.format(Occurred_time, Contract_code)

        cur.execute(Query)
        (res,) = cur.fetchone()
        df.loc[idx, 'Spot'] = res
    cur.close()
    con.close()
    return df


def query_latest_ts_futures(exchange, Contract_code):
    # unify the format !! IMPORTANT !!
    Contract_code = f"\'{Contract_code}\'"

    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )
    Query = '''
            SELECT MAX(occurred_time) from ts_futures
            WHERE contract_code = {}
            '''.format(Contract_code)
    cur = con.cursor()
    cur.execute(Query)
    (res,) = cur.fetchone()

    cur.close()
    con.close()
    return res


def query_latest_ts_option(exchange, Contract_code):
    # unify the format !! IMPORTANT !!
    Contract_code = f"\'{Contract_code}\'"

    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )
    Query = '''
        SELECT MAX(occurred_time) from ts_option
        WHERE contract_code = {}
        '''.format(Contract_code)
    cur = con.cursor()
    cur.execute(Query)
    (res,) = cur.fetchone()

    cur.close()
    con.close()
    return res


def query_ts_option(exchange, Currency, Occurred_time, Maturity):
    # unify the format !! IMPORTANT !!
    if not isinstance(Occurred_time, str):
        Occurred_time = dt.datetime.strftime(Occurred_time, '%Y-%m-%d %T')
    if not isinstance(Maturity, str):
        Maturity = dt.datetime.strftime(Maturity, '%Y-%m-%d %T')

    Occurred_time = "\'" + Occurred_time + "\'"
    Maturity = "\'" + Maturity + "\'"
    Currency = "\'" + Currency.upper() + "\'"

    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )
    cur = con.cursor()
    Query = """
        SELECT tso.Contract_code,
                tso.price AS premium,
                DATE_TRUNC('DAY',o.maturity) AS maturity, 
                DATE_TRUNC('DAY',tso.occurred_time) AS today, 
                tso.iv,
                tso.oi, 
                o.Option_type,
                o.strike,
                tsf.price AS spot 
        FROM ts_option AS tso
        JOIN option AS o
        ON tso.contract_code = o.contract_code
        JOIN ts_futures AS tsf
        ON tsf.contract_code = o.underlying_code and tso.occurred_time = tsf.occurred_time
        WHERE DATE_TRUNC('DAY',tso.occurred_time) = {} 
            AND o.currency_ticker = {} 
            AND DATE_TRUNC('DAY',o.maturity) = {}
        ORDER BY strike
    """.format(Occurred_time, Currency, Maturity)

    res = pd.read_sql_query(Query, con)
    res.columns = res.columns.map(lambda x: x.capitalize())
    res.rename(columns={'Iv': 'IV', 'Oi': "OI"}, inplace=True)
    cur.close()
    con.close()
    return res


def query_option_expiration(exchange, Currency, Occurred_time):
    # unify the format !! IMPORTANT !!
    if not isinstance(Occurred_time, str):
        Occurred_time = dt.datetime.strftime(Occurred_time, '%Y-%m-%d %T')
    Occurred_time = "\'" + Occurred_time + "\'"
    Currency = "\'" + Currency.upper() + "\'"

    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )
    Query = '''
            SELECT DISTINCT DATE_TRUNC('DAY', o.maturity) AS maturity
            FROM option AS o
            JOIN ts_option AS tso
            ON tso.contract_code = o.contract_code
            WHERE DATE_TRUNC('DAY',tso.occurred_time) = {} 
                AND o.currency_ticker = {}                 
            ORDER BY DATE_TRUNC('DAY', o.maturity)
        '''.format(Occurred_time, Currency)
    res = pd.read_sql_query(Query, con)
    res.rename(columns={'maturity': 'Maturity'}, inplace=True)
    con.close()
    return res


def query_max_pain(exchange, currency, occurred_time, limit=None):
    # unify the format !! IMPORTANT !!
    if not isinstance(occurred_time, str):
        occurred_time = dt.datetime.strftime(occurred_time, '%Y-%m-%d %T')
    occurred_time = "\'" + occurred_time + "\'"
    currency = "\'" + currency.upper() + "\'"

    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )
    Query = '''
    SELECT o.maturity, tso.oi, o.strike, o.option_type
    FROM ts_option AS tso
    JOIN option AS o
    ON o.contract_code = tso.contract_code
    JOIN futures AS f 
    ON o.underlying_code = f.contract_code
    WHERE DATE_TRUNC('DAY',tso.occurred_time) = {}  AND f.currency_ticker = {} 
    '''.format(occurred_time, currency)

    res = pd.read_sql_query(Query, con)
    res.columns = ["Maturity", "OI", 'Strike', 'Option_type']
    con.close()
    return res


def query_ts_option_for_vol_cone(exchange, Currency, Occurred_time):
    # unify the format !! IMPORTANT !!
    if not isinstance(Occurred_time, str):
        Occurred_time = dt.datetime.strftime(Occurred_time, '%Y-%m-%d %T')

    Occurred_time = "\'" + Occurred_time + "\'"
    Currency = "\'" + Currency.upper() + "\'"

    # connect the server
    con = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE[exchange]
    )
    cur = con.cursor()
    Query = """
        SELECT tso.Contract_code,
                tso.iv,
                o.strike,
                tsf.price AS spot ,
				DATE_PART('day',o.maturity-tso.occurred_time) AS time_to_maturity
        FROM ts_option AS tso
        JOIN option AS o
        ON tso.contract_code = o.contract_code
        JOIN ts_futures AS tsf
        ON tsf.contract_code = o.underlying_code and tso.occurred_time = tsf.occurred_time
        WHERE DATE_TRUNC('DAY',tso.occurred_time) = {}
            AND o.currency_ticker = {}
            AND o.option_type = 'call'
        ORDER BY time_to_maturity, strike
    """.format(Occurred_time, Currency)

    res = pd.read_sql_query(Query, con)
    res.columns = res.columns.map(lambda x: x.capitalize())
    res.rename(columns={'Iv': 'IV'}, inplace=True)
    cur.close()
    con.close()
    return res

def df_concat_2ts(ts1, ts2, name1, name2):
    in1 = pd.Series(ts1, index=ts1.index, name=name1)
    in2 = pd.Series(ts2, index=ts2.index, name=name2)
    df_out = pd.concat([in1, in2], axis=1)
    return df_out

def df_concat_basis(ts1, ts2, name1, name2, name3, col_base):
    df_in = df_concat_2ts(ts1=ts1, ts2=ts2, name1=name1, name2=name2)
    col_target = (1 - col_base)
    underlying = df_in.iloc[:,col_base]
    target = df_in.iloc[:, col_target]
    in3 = pd.Series((target - underlying) / underlying, name=name3)
    df_out = pd.concat([df_in, in3], axis=1)
    return df_out

if __name__ == "__main__":
    # Initialization
    # currency = {'BTC': "Bitcoin",
    #             'ETH': "Ethereum",
    #             }
    #
    # initialize(exchange='deribit', currency=currency)

    # query
    col_base = 0
    name_exchange = 'deribit'
    date_beg = '2021-08-01'
    date_end = '2021-08-05'
    char_freq = '1min'
    # name_underlying = 'BTC'
    name_underlying = 'ETH'
    # name_fut = 'BTC-25JUN21'
    name_fut = 'ETH-24SEP21'
    name_basis = f'{name_fut} Basis over {name_underlying} (%)'
    futures = query_trades_futures(exchange=name_exchange, currency=name_underlying,
                                   start=date_beg, end=date_end, freq=char_freq)
    print(futures.keys())

    index = query_index_price(exchange=name_exchange, currency=name_underlying,
                              start=date_beg, end=date_end, freq=char_freq)
    print(index)

    ts1 = index['close']
    ts2 = futures[name_fut]['close']
    name1 = name_underlying
    name2 = name_fut
    if col_base == 1:
        ts1 = futures[name_fut]['close']
        ts2 = index['close']
        name1 = name_fut
        name2 = name_underlying
    df_ts = df_concat_basis(ts1=ts1, ts2=ts2, name1=name1, name2=name2,
                            name3=name_basis, col_base=col_base)