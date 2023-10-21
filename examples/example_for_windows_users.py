# -*- coding: utf-8 -*-
"""
Created on Sat Oct 21 16:54:54 2023
by A TOTAL PYTHON AND GITHUB NOOB. I arrived at the following after getting errors and asking chatgpt..

Also:
In class BinanceDataDumper I have changed the following:
url = 'http://ipinfo.io/json' # removed https due to ssl error
"""
import multiprocessing
from binance_historical_data import BinanceDataDumper

if __name__ == '__main__':
    # Add the following line to ensure safe multiprocessing on Windows
    multiprocessing.freeze_support() #according to chatgpt this is required to run properly on windows

    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=".",
        asset_class="spot",  # spot, um, cm
        data_type="klines",  # aggTrades, klines, trades
        data_frequency="1d",
    )

    # tickers=data_dumper.get_list_all_trading_pairs()


    data_dumper.dump_data(
        tickers="BTCUSDT",
        date_start=None,
        date_end=None,
        is_to_update_existing=False,
        tickers_to_exclude=["UST"],
    )

    print("success")

