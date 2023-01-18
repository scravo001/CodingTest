import re
import pandas as pd
import numpy as np
import time
from datetime import datetime


MONTH_CODES = "FGHJKMNQUVXZ"

MONTH_NAMES = [
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
]

MONTH_NUMS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

MONTH_NAME_TO_CODE = {k: v for k, v in zip(MONTH_NAMES, MONTH_CODES)}

FIELDS_MAP = {
    "Trade Date": "date",
    "Risk Free Interest Rate": "RATE",
    "Open Implied Volatility": "PRICE_OPEN",
    "Last Implied Volatility": "PRICE_LAST",
    "High Implied Volatility": "PRICE_HIGH",
    "Previous Close Price": "PRICE_CLOSE_PREV",
    "Close Implied Volatility": "IMPLIEDVOL_BLACK",
    "Strike Price": "STRIKE",
    "Option Premium": "PREMIUM",
    "General Value6": "UNDL_PRICE_SETTLE",
    "General Value7": "UNDL_PRICE_LAST",
}

FLOAT_FIELDS = [
    "PRICE_OPEN",
    "PRICE_LAST",
    "PRICE_HIGH",
    "PRICE_CLOSE_PREV",
    "IMPLIEDVOL_BLACK",
    "PREMIUM",
    "RATE",
    "STRIKE",
    "UNDL_PRICE_SETTLE",
    "UNDL_PRICE_LAST",
]

#def extract_moneyness(ric):
#match = re.search(r"\d+", ric)
#return match.group()
#match = re.match(r"(^\w+)(\d+)", ric)
#match = re.match(r"(?:\D*)(\d)(?:\D*)", ric)
#return match.groups()

def parse(ric):
    ric = ric.split("N")[0]
    if ric[2].isalpha():
        base = ric[:3]
        m = ric[3:]
    else:
        base = ric[:2]
        m = ric[2:]
    return base,m

def parse_base(ric):
    base,_ = parse(ric)
    return base

def parse_m(ric):
    _,m = parse(ric)
    return m

    #match = re.match(r"(^\w+)(\d+)", ric)
    #base, moneyness = match.groups()
    #return [moneyness,base]    

def transform(df: pd.DataFrame, instruments_: pd.DataFrame) -> pd.DataFrame:
    """
    Create a function called transform that returns a normalized table.
    Do not mutate the input.
    The runtime of the transform function should be below 1 second.

    :param raw_data_: dataframe of all features associated with instruments, with associated timestamps
    :param instruments_: dataframe of all traded instruments
    """
    print("transform..")

    # User Defined Identifier
    # Instrument Name
    # RIC
    # Trade Date
    # Trade Time
    # Expiration Date
    # Period
    # Term
    # Volatility Surface Term
    # Risk Free Interest Rate
    # Open Implied Volatility
    # Last Implied Volatility
    # High Implied Volatility
    # Close Implied Volatility
    # Strike Price
    # Option Premium
    # Premium
    # General Value6
    # General Value7
    # Precision
    # Previous Close Date
    # Previous Close Price
    # Last Update Time
    # Instrument Snap Time
    # Asset Type Description
    # Contributor Short Name
    # Exchange Description
    # Currency Code
    # Embargo Times
    # Maximum Embargo Delay
    # Real Time Permitted

    #1 Drop any rows with "Not Found" in the Error column. The Error column may or may not be in the columns.
    # error col doesnt exist
    #rd = df[df['Error'] != 'Not Found']

    #2 Create a column called contract, which is a copy of the Term column. If there are nulls in the Term column, fill it with the Period column.
    df = df.assign(contract = df['Term'])
    df['contract'] = df['contract'].fillna(df['Period'])
    #3 Check for nulls in Trade Date column. 
    # Raise a warning if any nulls exist and drop the rows with null Trade Date.
    #null_mask = df.isna(df["Trade Date"])
    null_mask = df["Trade Date"].isna()
    if null_mask.any():
        print("Warning: Null values found in 'Trade Date' column.")

    df = df[~null_mask]

    #4. Check for expired instruments. An instrument is expired if the Expiration Date is older than the Trade Date. Raise a warning if there are expired instruments and drop the rows with expired instruments.
    df['Trade Date'] = pd.to_datetime(df['Trade Date'])
    df['Expiration Date'] = pd.to_datetime(df['Expiration Date'])

    # check for expired instruments
    expired_instruments = df[df['Expiration Date'] < df['Trade Date']]

    # raise warning if there are expired instruments
    if not expired_instruments.empty:
        print('Warning: There are expired instruments')

    #5. Check for nulls in contract column. Raise a warning if nulls exist and drop the rows with null contract name.
    null_contracts = df[df['Instrument Name'].isnull()]
    if not null_contracts.empty:
        print('Warning: There are null contract names')

    # drop rows with null contract names
    df = df.dropna(subset=['Instrument Name'])

    # 6. Parse the RIC base and moneyness from the RIC column and merge instruments dataframe on the base.

    # Example: 1BO50Nc1=R -> moneyness = 50, base=1BO    
    try:
        df['Base'] = df['RIC'].apply(parse_base)
        df['Moneyness'] = df['RIC'].apply(parse_m)
    except Exception as e:
        print(e)
        return

    # Example: 1BO100Nc1O=R -> moneyness = 100, base=1BO

    # 7. Create two columns called contract_year and contract_month. For contract_year, parse the year
    # from Expiration Date. 
    # For contract_month, parse the month from Period (JAN, FEB, etc.). 

    # If the last digit of the contract_year does not equal the last digit of the Period column, 
    # increment contract_year by 1. 
    # You may or may not see Jan/Feb contracts that do not match the year of the expiration date. 
    # For instance, a Jan 2021 contract may expire in December 19, 2020.
    df['Expiration Date'] = pd.to_datetime(df['Expiration Date'])
    df['contract_year'] = df['Expiration Date'].dt.year

    df['contract_month'] = df['Period'].apply(lambda x: x[:3])
    #df['contract_month'] = df['Period']
    #print(df["contract_month"])

    df['check_digit_contract_year'] = df['contract_year'].apply(lambda x: x%10)
    df['check_digit_Period'] = df['Period'].apply(lambda x: x[-1])
    df['compare'] = df.apply(lambda x: x['check_digit_contract_year'] != int(x['check_digit_Period']), axis=1)
    #print(df['check_digit_contract_year'])
    #print(df['check_digit_Period'])
    #count = df['compare'].sum()
    #print("c: ",count)

    df.loc[df['compare'] == True, 'contract_year'] += 1

    # 8. Create a column called month_code that maps contract_month using the MONTH_NAME_TO_CODE dict.
    #df['month_code'] = df['contract_month'].apply(MONTH_NAME_TO_CODE)
    df['month_code'] = df['contract_month'].map(MONTH_NAME_TO_CODE)

    # 9. Rename some columns using FIELD_MAP.
    df.rename(columns=FIELDS_MAP, inplace=True)

    # 10. Create a column called symbol that is a concatenation of "FUTURE_VOL_", Exchange, Bloomberg
    #Blooomberg = Currency Code??
    #df = df.assign(symbol=lambda x: 'FUTURE_VOL_' + x['Exchange Description'] + '_' + x['Currency Code'])
    df = df.assign(symbol=lambda x: 'FUTURE_VOL_' + x['Contributor Short Name'])

    # Ticker, month_code, contract_year, and moneyness. Single character Bloomberg Tickers are special.

    #Transform the rest of the raw data to match the expected output.
    #Hint: Start with the FLOAT_FIELDS list to extract contract values, and ensure that the column types are appropriate.
    return df

def process():
    raw_data = pd.read_csv("raw_data.csv")
    instruments = pd.read_csv("instruments.csv")
    st = time.process_time()
    output = transform(raw_data, instruments)
    #,date,symbol,source,field,value
    #source,field,value dont exist
    foutput = output[["Expiration Date", "symbol", "Base", "Moneyness","value"]]
    foutput.to_csv("newdata.csv",index=False)
    et = time.process_time()
    print(f"Wall time: {100 * (et-st)} ms")

    # expected_output = pd.read_csv(
    #     "expected_output.csv",
    #     index_col=0,
    #     parse_dates=['date']
    # )
    # pd.testing.assert_frame_equal(output, expected_output)


if __name__ == '__main__':
    process()
