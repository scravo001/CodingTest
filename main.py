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


def transform(raw_data_: pd.DataFrame, instruments_: pd.DataFrame) -> pd.DataFrame:
    """
    Create a function called transform that returns a normalized table.
    Do not mutate the input.
    The runtime of the transform function should be below 1 second.

    :param raw_data_: dataframe of all features associated with instruments, with associated timestamps
    :param instruments_: dataframe of all traded instruments
    """
    pass


if __name__ == '__main__':
    raw_data = pd.read_csv("raw_data.csv")
    instruments = pd.read_csv("instruments.csv")
    st = time.process_time()
    output = transform(raw_data, instruments)
    et = time.process_time()
    print(f"Wall time: {100 * (et-st)} ms")
    expected_output = pd.read_csv(
        "expected_output.csv",
        index_col=0,
        parse_dates=['date']
    )
    pd.testing.assert_frame_equal(output, expected_output)