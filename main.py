from locale import D_FMT
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import logging

def getInfo(ticker: str):
    """getInfo creates a Ticker object, then returns a pandas df of all the available info
            
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


def dictToDF(dictionary: dict):
    """The dictToDF method takes in a dictionary and returns a Pandas DataFrame
    
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


def generalInfo(ticker: str):
    """
    The generalInfo method takes in any Ticker and returns the general info associated with that ticker
    
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
                
    general = dictToDF(general_info)
    
    return general


def getFinancials(ticker: str):
    """getFinancials returns a dataframe with a lot of useful financial information
    
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


def toSQL(engine, tableName, df):
    """toSQL creates a table in a MySQL server to store a given Pandas DataFrame

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


def getHistoricPrice(ticker: str):
    """getHistoricPrice returns a dataframe with the historic stock price of the given ticker
    
        Parameters:
                ticker: String
                
        Returns:
                A Pandas DataFrame of the ticker's historic stock price
    """
    ticker = yf.Ticker(ticker.upper())
    hist = ticker.history(period='max')
    df = pd.DataFrame(hist)

    return df


def getDividends(ticker: str):
    """getDividends returns a dataframe with the all the dividends paid out for the given ticker
    
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
        all_info = getInfo(tick)
        tableName = "{} Info".format(tick)

        toSQL(sqlEngine, tableName, all_info)

    elif choice == 2:
        tick = str(input("For which ticker?\n")).upper()
        fin_info = getFinancials(tick)
        tableName = "{} Financial Info".format(tick)

        toSQL(sqlEngine, tableName, fin_info)

    elif choice == 3:
        tick = str(input("For which ticker?\n")).upper()
        gen_info = generalInfo(tick)
        tableName = "{} General Info".format(tick)

        toSQL(sqlEngine, tableName, gen_info)

    elif choice == 4:
        tick = str(input("For which ticker?\n")).upper()
        his_price = getHistoricPrice(tick)
        tableName = "{} Historic Price".format(tick)

        toSQL(sqlEngine, tableName, his_price)

    elif choice == 5:
        tick = str(input("For which ticker?\n")).upper()
        dividends = getDividends(tick)
        tableName = "{} Dividends".format(tick)

        toSQL(sqlEngine, tableName, dividends)

    else:
        raise Exception()


