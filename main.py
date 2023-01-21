import warnings
import pandas as pd
import numpy as np
import time

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


def pars(raw):
    ric = raw['RIC'].split('N')[0]
    raw['Base'] = ric[:3] if ric[2].isalpha() else ric[:2]
    raw['moneyness'] = ric[3:] if ric[2].isalpha() else ric[2:]
    return raw

def transform(raw: pd.DataFrame, inst: pd.DataFrame) -> pd.DataFrame:
    """
    Create a function called transform that returns a normalized table.
    Do not mutate the input.
    The runtime of the transform function should be below 1 second.

    :param raw: dataframe of all features associated with instruments, with associated timestamps
    :param inst: dataframe of all traded instruments
    """
    # 1. Drop any rows with "Not Found" in the Error column. The Error column may or may not be in the columns.
    # raw = raw[raw['Error'] != 'Not Found']
    # 2. Create a column called contract, which is a copy of the Term column. If there are nulls in the Term column, fill it with the Period column.
    raw['contract'] = raw['Term'].fillna(raw['Period'])
    if raw['Trade Date'].isna().any():
        warnings.warn("Trade Date column contains null values.")
        raw.dropna(subset=['Trade Date'], inplace=True)

    # 3. Check for nulls in Trade Date column. Raise a warning if any nulls exist and drop the rows with null Trade Date.
    null_trade_date = raw['Trade Date'].isna().any()
    if null_trade_date:
        warnings.warn("Trade Date column contains null values.")
        raw.dropna(subset=['Trade Date'], inplace=True)

    # 4. Check for expired instruments.
    # Convert columns to datetime
    raw['Expiration Date'] = pd.to_datetime(raw['Expiration Date'])
    raw['Trade Date'] = pd.to_datetime(raw['Trade Date'])

    # 5. Check for nulls in contract column. Raise a warning if nulls exist and drop the rows with null contract name.
    # Check for expired instruments
    expired_instruments = raw[raw['Expiration Date'] < raw['Trade Date']]
    if expired_instruments.shape[0] > 0:
        warnings.warn("There are expired instruments.")
        raw.drop(expired_instruments.index, inplace=True)

    if raw['contract'].isna().any():
        warnings.warn("contract column contains null values.")
        raw.dropna(subset=['contract'], inplace=True)

    # 6. Parse the RIC base and moneyness from the RIC column and merge instruments dataframe on the base.
    raw = raw.apply(pars, axis=1)

    # 7. Create two columns called contract_year and contract_month.
    # For contract_year, parse the year from Expiration Date.
    # For contract_month, parse the month from Period (JAN, FEB, etc.).
    # If the last digit of the contract_year does not equal the last digit of the Period column, increment
    # contract_year by 1.
    # You may or may not see Jan/Feb contracts that do not match the year of the expiration date.
    # For instance, a Jan 2021 contract may expire in December 19, 2020.

    # raw = pd.merge(raw, inst, on='Base', how='left')
    raw = raw.merge(inst, left_on='Base', right_on='Base', how='left')
    raw['Expiration Date'] = pd.to_datetime(raw['Expiration Date'])
    raw['contract_year'] = raw['Expiration Date'].dt.year
    raw['contract_month'] = raw['Period'].str[:3]
    raw['compare'] = (raw['contract_year'] % 10 != raw['Period'].str[-1].astype(int))
    raw.loc[raw['compare'] == True, 'contract_year'] += 1

    # 8. Create a column called month_code that maps contract_month using the
    raw['month_code'] = raw['contract_month'].map(MONTH_NAME_TO_CODE)

    # 9. Rename some columns using FIELD_MAP.
    raw = raw.rename(FIELDS_MAP, axis=1)

    # 10. Create a column called symbol that is a concatenation of "FUTURE_VOL_", Exchange, Bloomberg
    # Ticker, month_code, contract_year, and moneyness. Single character Bloomberg Tickers are
    # special
    # 11. Transform the rest of the raw data to match the expected output.
    # Hint: Start with the FLOAT_FIELDS list to extract contract values, and ensure that the column
    # types are appropriate.
    raw['symbol'] = 'FUTURE_VOL_' + raw['Exchange'] + '_' + raw['Bloomberg Ticker'] + raw['month_code'] + raw[
        'contract_year'].astype(str) + '_' + raw['moneyness']

    # Create a new dataframe with the desired columns
    output = pd.DataFrame(columns=['date', 'symbol', 'source', 'field', 'value'])

    # Extract the relevant data from the raw dataframe and append to the output dataframe
    temp_df = raw[['date', 'symbol', 'Contributor Short Name'] + FLOAT_FIELDS]
    temp_df = pd.melt(temp_df, id_vars=['date', 'symbol', 'Contributor Short Name'], value_vars=FLOAT_FIELDS,
                      var_name='field', value_name='value')
    temp_df.rename(columns={'Contributor Short Name': 'source'}, inplace=True)
    output = output.append(temp_df)
    output['date'] = pd.to_datetime(output['date'])

    return output


if __name__ == '__main__':
    raw = pd.read_csv("raw_data.csv")
    inst = pd.read_csv("instruments.csv")
    st = time.process_time()
    output = transform(raw, inst)
    et = time.process_time()
    print(f"Wall time: {100 * (et - st)} ms")
    expected_output = pd.read_csv(
        "expected_output.csv",
        index_col=0,
        parse_dates=['date']
    )
    pd.testing.assert_frame_equal(output, expected_output)