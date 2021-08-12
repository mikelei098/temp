import pandas as pd
import numpy as np


def read_data(filename):
    df = pd.read_csv(filename)
    return df


if __name__ == "__main__":
    df = read_data("trade_BTC-PERPETUAL.csv")