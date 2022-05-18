from locale import D_FMT
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import logging

def get_info(ticker: str):
    """get_info creates a Ticker object, then returns a pandas df of all the available info
            
        Parameters:
                ticker: String
                
        Returns:
                A Pandas DataFrame
    
    """
    keys = []
    values = []
    yfTicker = yf.Ticker(ticker.upper())
    info = yfTicker.info
    keys = info.keys()
    values = info.values()
    zipped = list(zip(keys,values))

    zipped.pop(149) # removes logo url to avoid SQL error
    zipped.pop(8) # removes website url to avoid SQL error
    zipped.pop(3) # removes longBusinessSummary to avoid SQL error
    
    df = pd.DataFrame(data=zipped, columns=['keys','values'])
    
    return df


def dict_to_df(dictionary: dict):
    """The dict_to_df method takes in a dictionary and returns a Pandas DataFrame
    
        Parameters:
                ticker: String
                
        Returns:
                A Pandas DataFrame
                
    """
    columns = ["keys","values"]
    keys = []
    values = []
    zipped = ()
    
    # Separates keys from values
    keys = dictionary.keys()
    
    # Separates values from keys
    values = dictionary.values()
    
    # Zips the keys and values into a tuple for easy viewing
    zipped = zip(keys,values)
    
    # Pandas DataFrame of all info from ticker
    df = pd.DataFrame(zipped,columns=columns)
    
    return df


def general_info(ticker: str):
    """
    The general_info method takes in any Ticker and returns the general info associated with that ticker
    
        Parameters:
                ticker: String
                
        Returns:
                A Pandas DataFrame
    """
    key_list = ["longName","symbol","market","industry","sector","country",
                "state","city","address1","zip","phone","website"]
    general_info = {}
    
    tick = yf.Ticker(ticker)
    info = tick.info
    
    for key in key_list:
        for i in info:
            if i == key:
                general_info[key] = info[i]
                
    general = dict_to_df(general_info)
    
    return general


def get_financials(ticker: str):
    """get_financials returns a dataframe with a lot of useful financial information
    
        Parameters:
                ticker: String
                
        Returns:
                A Pandas DataFrame of the ticker's financial information
    """
    ticker = yf.Ticker(ticker.upper())
    bs = ticker.balancesheet
    cf = ticker.cashflow
    fn = ticker.financials
    
    return pd.concat([bs,cf,fn])


def table_in_sql(engine, tableName, df):
    """table_in_sql creates a table in a MySQL server to store a given Pandas DataFrame

        Parameters:
                engine: A SQL Engine to connect to MySQL server
                tableName: Name of the table being created in MySQL server
                df: A Pandas DataFrame that consists of a user-selected choice
    """
    logging.basicConfig(filename='logfile.log', encoding='utf-8', level=logging.DEBUG)
    dbConnection = engine.connect()

    try:
        df.to_sql(tableName, dbConnection)
    except ValueError as vx:
        logging.info(vx)
    except Exception as ex:   
        logging.info(ex)
    else:
        print("Table %s created successfully."%tableName);   
    finally:
        dbConnection.close()


def get_historic_price(ticker: str):
    """get_historic_price returns a dataframe with the historic stock price of the given ticker
    
        Parameters:
                ticker: String
                
        Returns:
                A Pandas DataFrame of the ticker's historic stock price
    """
    ticker = yf.Ticker(ticker.upper())
    hist = ticker.history(period='max')
    df = pd.DataFrame(hist)

    return df


def get_dividends(ticker: str):
    """get_dividends returns a dataframe with the all the dividends paid out for the given ticker
    
        Parameters:
                ticker: String
                
        Returns:
                A Pandas DataFrame of the ticker's historic stock price
    """
    ticker = yf.Ticker(ticker.upper())
    div = ticker.dividends
    df = pd.DataFrame(div)

    return df


if __name__ == "__main__":
    sqlEngine = create_engine('mysql+pymysql://root:password@localhost:3306/stocks')

    print("Pick an option:")
    choice = int(input("""
        1: All Info
        2: Financial Info
        3: General Info
        4: Historic Price
        5: Dividends Paid Out
    """))

    if choice == 1:
        tick = str(input("For which ticker?\n")).upper()
        all_info = get_info(tick)
        tableName = "{} Info".format(tick)

        table_in_sql(sqlEngine, tableName, all_info)

    elif choice == 2:
        tick = str(input("For which ticker?\n")).upper()
        fin_info = get_financials(tick)
        tableName = "{} Financial Info".format(tick)

        table_in_sql(sqlEngine, tableName, fin_info)

    elif choice == 3:
        tick = str(input("For which ticker?\n")).upper()
        gen_info = general_info(tick)
        tableName = "{} General Info".format(tick)

        table_in_sql(sqlEngine, tableName, gen_info)

    elif choice == 4:
        tick = str(input("For which ticker?\n")).upper()
        his_price = get_historic_price(tick)
        tableName = "{} Historic Price".format(tick)

        table_in_sql(sqlEngine, tableName, his_price)

    elif choice == 5:
        tick = str(input("For which ticker?\n")).upper()
        dividends = get_dividends(tick)
        tableName = "{} Dividends".format(tick)

        table_in_sql(sqlEngine, tableName, dividends)

    else:
        raise Exception()


