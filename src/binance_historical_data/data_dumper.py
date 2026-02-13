"""Module with class to download candle historical data from binance"""

# Standard library imports
import os
import re
import urllib.request
import xml.etree.ElementTree as ET
import json
import logging
from collections import defaultdict
from collections import Counter
import zipfile
import datetime
import time
from dateutil.relativedelta import relativedelta

# Third party imports
from tqdm.auto import tqdm
from char import char
from mpire import WorkerPool

from joblib import Parallel, delayed
from concurrent.futures import ThreadPoolExecutor, as_completed

# from tqdm.auto import tqdm #for jupyter autodetec - no moving bars! bad!
from tqdm import tqdm

# Global constants
# IMPORTANT: To see benchmark statistics and logging output, configure logging
# in your script BEFORE importing BinanceDataDumper:
#
# import logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('download.log', encoding='utf-8'),
#         logging.StreamHandler()  # Also print to console
#     ]
# )
# from binance_historical_data import BinanceDataDumper  # Import AFTER logging config
#
LOGGER = logging.getLogger(__name__)


class BinanceDataDumper:
    _FUTURES_ASSET_CLASSES = ("um", "cm")
    _ASSET_CLASSES = ("spot",)
    _DICT_DATA_TYPES_BY_ASSET = {
        "spot": ("aggTrades", "klines", "trades"),
        "cm": (
            "aggTrades",
            "fundingRate",
            "klines",
            "trades",
            "indexPriceKlines",
            "markPriceKlines",
            "premiumIndexKlines",
        ),
        "um": (
            "aggTrades",
            "bookDepth",
            "bookTicker",
            "fundingRate",
            "indexPriceKlines",
            "klines",
            "liquidationSnapshot",
            "markPriceKlines",
            "metrics",
            "premiumIndexKlines",
            "trades",
        ),
    }
    _DATA_FREQUENCY_NEEDED_FOR_TYPE = (
        "klines",
        "indexPriceKlines",
        "markPriceKlines",
        "premiumIndexKlines",
    )
    _DATA_FREQUENCY_ENUM = (
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1mo",
    )

    def __init__(
        self,
        path_dir_where_to_dump,
        asset_class="spot",  # spot, um, cm
        data_type="klines",  # aggTrades, klines, trades
        data_frequency="1m",  # argument for data_type="klines"
        max_concurrent_downloads=None,  # max concurrent file downloads
    ) -> None:
        """Init object to dump all data from binance servers

        Args:
            path_dir_where_to_dump (str): Folder where to dump data
            asset_class (str): Asset class which data to get [spot, um, cm]
            data_type (str): data type to dump: \
                spot: [aggTrades, klines, trades] \
                um/cm futures: [aggTrades, klines, trades, fundingRate, \
                indexPriceKlines, markPriceKlines, premiumIndexKlines, etc.]
            data_frequency (str): \
                Data frequency for klines/indexPriceKlines/markPriceKlines/premiumIndexKlines. \
                [1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo] \
                Defaults to "1m". Not required for fundingRate (always 8-hour snapshots).
            max_concurrent_downloads (int): \
                Maximum number of concurrent file downloads. \
                Defaults to 1 for 'trades' (large files), 5 for all others. \
                Community consensus: 5 is safe. Binance's own scripts use 2. \
                Circuit breaker protects against rate limiting. \
                Set explicitly to override auto-detection.
        """
        # Confirm editable install is working
        import os as _os_check
        file_mtime = _os_check.path.getmtime(__file__)
        file_time = datetime.datetime.fromtimestamp(file_mtime).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[BinanceDataDumper] Loaded from: {__file__}")
        print(f"[BinanceDataDumper] File last modified: {file_time}")
        print(f"[BinanceDataDumper] Benchmarking and concurrency features enabled")

        if asset_class not in (self._ASSET_CLASSES + self._FUTURES_ASSET_CLASSES):
            raise ValueError(
                f"Unknown asset class: {asset_class} "
                f"not in {self._ASSET_CLASSES + self._FUTURES_ASSET_CLASSES}"
            )

        if data_type not in self._DICT_DATA_TYPES_BY_ASSET[asset_class]:
            raise ValueError(
                f"Unknown data type: {data_type} "
                f"not in {self._DICT_DATA_TYPES_BY_ASSET[asset_class]}"
            )

        if data_type in self._DATA_FREQUENCY_NEEDED_FOR_TYPE:
            if data_frequency not in self._DATA_FREQUENCY_ENUM:
                raise ValueError(
                    f"Unknown data frequency: {data_frequency} "
                    f"not in {self._DATA_FREQUENCY_ENUM}"
                )
            self._data_frequency = data_frequency
        else:
            self._data_frequency = ""

        self.path_dir_where_to_dump = path_dir_where_to_dump
        self.dict_new_points_saved_by_ticker = defaultdict(dict)
        self._base_url = "https://data.binance.vision/data"
        self._asset_class = asset_class
        self._data_type = data_type

        # Auto-detect concurrent downloads if not specified
        if max_concurrent_downloads is None:
            # Only 'trades' (not 'aggTrades') should default to 1
            # Use 5 for others (conservative to avoid Binance throttling)
            self._max_concurrent_downloads = 1 if data_type == "trades" else 5
        else:
            self._max_concurrent_downloads = max_concurrent_downloads

        # Circuit breaker pattern to prevent hammering during rate limiting
        self._circuit_breaker_failures = 0
        self._circuit_breaker_threshold = 5  # Stop after 5 consecutive failures
        self._circuit_breaker_reset_time = 300  # 5 minutes
        self._circuit_breaker_last_failure = None
        self._is_circuit_broken = False

        # Request tracking and benchmarking
        self._request_count = 0
        self._successful_requests = 0
        self._failed_requests = 0
        self._not_found_requests = 0  # Track 404 errors separately
        self._active_connections = 0
        self._max_concurrent_connections = 0
        self._request_start_time = None
        self._request_timings = []  # Store individual request durations
        self._bytes_downloaded = 0

    @char
    def dump_data(
        self,
        tickers=None,
        date_start=None,
        date_end=None,
        is_to_update_existing=False,
        int_max_tickers_to_get=None,
        tickers_to_exclude=None,
        print_benchmark=True,
    ):
        """Main method to dump new of update existing historical data

        Args:
            tickers (list[str]):\
                list trading pairs for which to dump data\
                by default all ****USDT pairs will be taken
            tickers_to_exclude (list[str]):\
                list trading pairs which to exclude from dump
            date_start (datetime.date): Date from which to start dump
            date_end (datetime.date): The last date for which to dump data
            print_benchmark (bool): Whether to print benchmark stats after this dump.\
                Set to False when dumping multiple tickers in a loop, then call\
                print_final_benchmark() at the end for cumulative stats. Defaults to True.
            is_to_update_existing (bool): \
                Flag if you want to update data if it's already exists
            int_max_tickers_to_get (int): Max number of trading pairs to get
        """
        self.dict_new_points_saved_by_ticker.clear()
        list_trading_pairs = self._get_list_trading_pairs_to_download(
            tickers=tickers, tickers_to_exclude=tickers_to_exclude
        )
        if int_max_tickers_to_get:
            list_trading_pairs = list_trading_pairs[:int_max_tickers_to_get]
        LOGGER.info("Download full data for %d tickers: ", len(list_trading_pairs))

        LOGGER.info(
            "---> Data will be saved here: %s",
            os.path.join(
                os.path.abspath(self.path_dir_where_to_dump), self._asset_class
            ),
        )

        LOGGER.info("---> Data Frequency: %s", self._data_frequency)
        # Start date
        if date_start is None:
            date_start = datetime.date(year=2017, month=1, day=1)
        if date_start < datetime.date(year=2017, month=1, day=1):
            date_start = datetime.date(year=2017, month=1, day=1)
        # End date
        if date_end is None:
            date_end = datetime.datetime.utcnow().date() - relativedelta(days=1)
        if date_end > datetime.datetime.utcnow().date() - relativedelta(days=1):
            date_end = datetime.datetime.utcnow().date() - relativedelta(days=1)
        LOGGER.info("---> Start Date: %s", date_start.strftime("%Y%m%d"))
        LOGGER.info("---> End Date: %s", date_end.strftime("%Y%m%d"))
        date_end_first_day_of_month = datetime.date(
            year=date_end.year, month=date_end.month, day=1
        )

        # Create progress bar (update manually so it actually works!)
        # with tqdm(
        #     total=len(list_trading_pairs), desc="Tickers", position=0, leave=True
        # ) as pbar:

        #     # Tell mpire to position its progress bars below with position=1
        #     os.environ["MPIRE_PROGRESS_BAR_POSITION"] = "1"

        #     for ticker in list_trading_pairs:

        print(f"doing tickers: {len(list_trading_pairs)}")
        # for ticker in tqdm(
        #     list_trading_pairs, desc="Processing tickers", unit="ticker"
        # ):

        # with tqdm(
        #     total=len(list_trading_pairs), desc="Processing tickers", unit="ticker"
        # ) as pbar:

        with tqdm(
            total=len(list_trading_pairs),
            desc="Processing tickers",
            unit="ticker",
            position=0,  # Fixed position at top
            leave=True,  # Keep the bar after completion
        ) as pbar:

            # with tqdm(
            #     total=len(list_trading_pairs), desc="Processing tickers", unit="ticker"
            # ) as pbar:

            for ticker in list_trading_pairs:
                # for ticker in list_trading_pairs:
                # tqdm(list_trading_pairs, leave=True, desc="Tickers"):

                # 1) Download all monthly data
                if (
                    self._data_type != "metrics"
                    and (  # is fullmonth end > start target?
                        date_end_first_day_of_month - relativedelta(days=1) > date_start
                    )
                ):
                    self._download_data_for_1_ticker(
                        ticker=ticker,
                        date_start=date_start,
                        date_end=(date_end_first_day_of_month - relativedelta(days=1)),
                        timeperiod_per_file="monthly",
                        is_to_update_existing=is_to_update_existing,
                    )
                # 2) Download all daily date
                if self._data_type == "metrics":
                    date_start_daily = date_start
                else:
                    date_start_daily = date_end_first_day_of_month
                self._download_data_for_1_ticker(
                    ticker=ticker,
                    date_start=date_start_daily,
                    date_end=date_end,
                    timeperiod_per_file="daily",
                    is_to_update_existing=is_to_update_existing,
                )

                # # Update ticker progress
                pbar.update(1)
                pbar.set_description(f"Processing {ticker}")
                # print(f"processed {ticker}")

                # Reset the mpire progress bar position
                # os.environ.pop("MPIRE_PROGRESS_BAR_POSITION", None)

        #####
        # Print statistics
        self._print_dump_statistics(print_benchmark=print_benchmark)

    def dump_specific_pairs(
        self,
        pairs_to_dump,
        is_to_update_existing=False,
        print_benchmark=True,
    ):
        """Dump data for specific symbol-date pairs

        Args:
            pairs_to_dump: List of (symbol, date) tuples or dict mapping symbols to date lists
            is_to_update_existing (bool): Whether to update existing files
            print_benchmark (bool): Whether to print benchmark stats after this dump.
                Defaults to True.
        """
        self.dict_new_points_saved_by_ticker.clear()

        # Standardize input format
        pairs_by_symbol = defaultdict(list)
        if isinstance(pairs_to_dump, dict):
            pairs_by_symbol.update(pairs_to_dump)
        else:
            for symbol, date in pairs_to_dump:
                pairs_by_symbol[symbol].append(date)

        with tqdm(
            total=len(pairs_by_symbol),
            desc="Processing symbols",
            position=0,
            leave=True,
        ) as pbar:
            for symbol, dates in pairs_by_symbol.items():
                # Create directory structure
                dir_where_to_save = self.get_local_dir_to_data(symbol, "daily")
                if not os.path.exists(dir_where_to_save):
                    try:
                        os.makedirs(dir_where_to_save)
                    except FileExistsError:
                        pass

                processes = self._max_concurrent_downloads
                list_args = [(symbol, date, "daily") for date in dates]

                if list_args:
                    list_saved_dates = list(
                        tqdm(
                            Parallel(
                                n_jobs=processes,
                                return_as="generator",
                                backend="threading",  # Threading: same speed as loky + benchmarks work
                                verbose=0
                            )(
                                delayed(self._download_data_for_1_ticker_1_date)(
                                    symbol, date, "daily"
                                )
                                for symbol, date, _ in list_args
                            ),
                            total=len(list_args),
                            desc=f"Daily files ({symbol})",
                            position=1,
                            leave=False,
                        )
                    )

                    self.dict_new_points_saved_by_ticker[symbol]["daily"] = len(
                        [d for d in list_saved_dates if d]
                    )

                pbar.update(1)

            self._print_dump_statistics(print_benchmark=print_benchmark)

    def get_list_all_trading_pairs(self):
        """Get all trading pairs available at binance now"""
        # Select the right Top Level Domain for US/non US
        country_code = self._get_user_country_from_ip()
        if country_code == "US":
            tld = "us"
        else:
            tld = "com"
        #####
        if self._asset_class == "um":
            response = urllib.request.urlopen(
                f"https://fapi.binance.{tld}/fapi/v1/exchangeInfo"
            ).read()
        elif self._asset_class == "cm":
            response = urllib.request.urlopen(
                f"https://dapi.binance.{tld}/dapi/v1/exchangeInfo"
            ).read()
        else:
            # https://api.binance.us/api/v3/exchangeInfo
            response = urllib.request.urlopen(
                f"https://api.binance.{tld}/api/v3/exchangeInfo"
            ).read()
        return list(
            map(lambda symbol: symbol["symbol"], json.loads(response)["symbols"])
        )

    @staticmethod
    def _get_user_country_from_ip() -> str:
        """Get user country to select the right binance url"""
        # remove https if  ssl error! url = 'http://ipinfo.io/json'
        url = "https://ipinfo.io/json"
        res = urllib.request.urlopen(url)
        data = json.load(res)
        return data.get("country", "Unknown")

    def _get_list_all_available_files(self, prefix=""):
        """Get all available files from the binance servers"""
        url = (
            os.path.join(self._base_url, prefix)
            .replace("\\", "/")
            .replace("data/", "?prefix=data/")
        )
        response = urllib.request.urlopen(url)
        html_content = response.read().decode("utf-8")

        # Extract the BUCKET_URL variable
        bucket_url_pattern = re.compile(r"var BUCKET_URL = '(.*?)';")
        match = bucket_url_pattern.search(html_content)

        if match:
            bucket_url = (
                match.group(1)
                + "?delimiter=/&prefix=data/"
                + prefix.replace("\\", "/")
                + "/"
            )

            # Retrieve the content of the BUCKET_URL
            bucket_response = urllib.request.urlopen(bucket_url)
            bucket_content = bucket_response.read().decode("utf-8")

            # Parse the XML content and extract all <Key> elements
            root = ET.fromstring(bucket_content)
            # Automatically retrieve the namespace
            namespace = {"s3": root.tag.split("}")[0].strip("{")}
            keys = [element.text for element in root.findall(".//s3:Key", namespace)]
            return keys
        else:
            raise ValueError("BUCKET_URL not found")

    def get_min_start_date_for_ticker(self, ticker):
        """Get minimum start date for ticker"""
        path_folder_prefix = self._get_path_suffix_to_dir_with_data("monthly", ticker)
        min_date = datetime.datetime(
            datetime.datetime.today().year, datetime.datetime.today().month, 1
        )

        try:
            date_found = False

            files = self._get_list_all_available_files(prefix=path_folder_prefix)
            for file in files:
                date_str = file.split(".")[0].split("-")[-2:]
                date_str = "-".join(date_str)
                date_obj = datetime.datetime.strptime(date_str, "%Y-%m")
                if date_obj < min_date:
                    date_found = True
                    min_date = date_obj

            if not date_found:
                path_folder_prefix = self._get_path_suffix_to_dir_with_data(
                    "daily", ticker
                )
                files = self._get_list_all_available_files(prefix=path_folder_prefix)
                for file in files:
                    date_str = file.split(".")[0].split("-")[-3:]
                    date_str = "-".join(date_str)
                    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                    if date_obj < min_date:
                        min_date = date_obj

        except Exception as e:
            LOGGER.error("Min date not found: ", e)

        return min_date.date()

    def get_local_dir_to_data(self, ticker, timeperiod_per_file):
        """Path to directory where ticker data is saved

        Args:
            ticker (str): trading pair
            timeperiod_per_file (str): timeperiod per 1 file in [daily, monthly]

        Returns:
            str: path to folder where to save data
        """
        path_folder_suffix = self._get_path_suffix_to_dir_with_data(
            timeperiod_per_file, ticker
        )
        return os.path.join(self.path_dir_where_to_dump, path_folder_suffix)

    @char
    def create_filename(
        self,
        ticker,
        date_obj,
        timeperiod_per_file="monthly",
        extension="csv",
    ):
        """Create file name in the format it's named on the binance server"""

        if timeperiod_per_file == "monthly":
            str_date = date_obj.strftime("%Y-%m")
        else:
            str_date = date_obj.strftime("%Y-%m-%d")

        return f"{ticker}-{self._data_frequency if self._data_frequency else self._data_type}-{str_date}.{extension}"

    def get_all_dates_with_data_for_ticker(self, ticker, timeperiod_per_file="monthly"):
        """Get list with all dates for which there is saved data

        Args:
            ticker (str): trading pair
            timeperiod_per_file (str): timeperiod per 1 file in [daily, monthly]

        Returns:
            list[datetime.date]: dates with saved data
        """
        date_start = datetime.date(year=2017, month=1, day=1)
        date_end = datetime.datetime.utcnow().date()
        list_dates = self._create_list_dates_for_timeperiod(
            date_start=date_start,
            date_end=date_end,
            timeperiod_per_file=timeperiod_per_file,
        )
        list_dates_with_data = []
        path_folder_suffix = self._get_path_suffix_to_dir_with_data(
            timeperiod_per_file, ticker
        )
        str_dir_where_to_save = os.path.join(
            self.path_dir_where_to_dump, path_folder_suffix
        )
        for date_obj in list_dates:
            file_name = self.create_filename(
                ticker,
                date_obj,
                timeperiod_per_file=timeperiod_per_file,
                extension="csv",
            )
            path_where_to_save = os.path.join(str_dir_where_to_save, file_name)
            if os.path.exists(path_where_to_save):
                list_dates_with_data.append(date_obj)

        return list_dates_with_data

    def get_all_tickers_with_data(self, timeperiod_per_file="daily"):
        """Get all tickers for which data was dumped

        Args:
            timeperiod_per_file (str): timeperiod per 1 file in [daily, monthly]

        Returns:
            list[str]: all tickers with data
        """
        folder_path = os.path.join(self.path_dir_where_to_dump, self._asset_class)
        folder_path = os.path.join(folder_path, timeperiod_per_file)
        folder_path = os.path.join(folder_path, self._data_type)
        tickers = [
            d
            for d in os.listdir(folder_path)
            if os.path.isdir(os.path.join(folder_path, d))
        ]
        return tickers

    def delete_outdated_daily_results(self):
        """
        Deleta daily data for which full month monthly data was already dumped
        """
        LOGGER.info("Delete old daily data for which there is monthly data")
        dict_files_deleted_by_ticker = defaultdict(int)
        tickers = self.get_all_tickers_with_data(timeperiod_per_file="daily")
        for ticker in tqdm(tickers, leave=False):
            list_saved_months_dates = self.get_all_dates_with_data_for_ticker(
                ticker, timeperiod_per_file="monthly"
            )
            list_saved_days_dates = self.get_all_dates_with_data_for_ticker(
                ticker, timeperiod_per_file="daily"
            )
            for date_saved_day in list_saved_days_dates:
                date_saved_day_tmp = date_saved_day.replace(day=1)
                if date_saved_day_tmp not in list_saved_months_dates:
                    continue
                str_folder = self.get_local_dir_to_data(
                    ticker,
                    timeperiod_per_file="daily",
                )
                str_filename = self.create_filename(
                    ticker,
                    date_saved_day,
                    timeperiod_per_file="daily",
                    extension="csv",
                )
                try:
                    os.remove(os.path.join(str_folder, str_filename))
                    dict_files_deleted_by_ticker[ticker] += 1
                except Exception:
                    LOGGER.warning(
                        "Unable to delete file: %s",
                        os.path.join(str_folder, str_filename),
                    )
        LOGGER.info(
            "---> Done. Daily files deleted for %d tickers",
            len(dict_files_deleted_by_ticker),
        )

    @char
    def _download_data_for_1_ticker(
        self,
        ticker,
        date_start,
        date_end=None,
        timeperiod_per_file="monthly",
        is_to_update_existing=False,
    ):
        """Dump data for 1 ticker"""
        min_start_date = self.get_min_start_date_for_ticker(ticker)
        LOGGER.debug(
            "Min Start date for ticker %s is %s",
            ticker,
            min_start_date.strftime("%Y%m%d"),
        )
        if date_start < min_start_date:
            date_start = min_start_date
            LOGGER.debug(
                "Start date for ticker %s is %s", ticker, date_start.strftime("%Y%m%d")
            )
        # Create list dates to use
        list_dates = self._create_list_dates_for_timeperiod(
            date_start=date_start,
            date_end=date_end,
            timeperiod_per_file=timeperiod_per_file,
        )
        LOGGER.debug("Created dates to dump data: %d", len(list_dates))
        list_dates_with_data = self.get_all_dates_with_data_for_ticker(
            ticker, timeperiod_per_file=timeperiod_per_file
        )
        LOGGER.debug("Dates with data found: %d", len(list_dates_with_data))
        if is_to_update_existing:
            list_dates_cleared = list_dates
        else:
            list_dates_cleared = [
                date_obj
                for date_obj in list_dates
                if date_obj not in list_dates_with_data
            ]

        if not list_dates_cleared:
            LOGGER.debug("No new dates to download for %s", ticker)
            self.dict_new_points_saved_by_ticker[ticker][timeperiod_per_file] = 0
            return

        LOGGER.debug("Dates to get data: %d", len(list_dates_cleared))
        list_args = [
            (ticker, date_obj, timeperiod_per_file) for date_obj in list_dates_cleared
        ]
        # 2) Create path to file where to save data
        dir_where_to_save = self.get_local_dir_to_data(
            ticker,
            timeperiod_per_file=timeperiod_per_file,
        )
        LOGGER.debug("Local dir to where dump: %s", dir_where_to_save)
        if not os.path.exists(dir_where_to_save):
            try:
                os.makedirs(dir_where_to_save)
            except FileExistsError:
                pass
        #####
        processes = min(
            len(list_args), self._max_concurrent_downloads
        )

        # if list_args:  # Only create progress bar if there are files to download
        #     list_saved_dates = list(
        #         tqdm(
        #             Parallel(
        #                 n_jobs=processes,
        #                 return_as="generator",
        #                 backend="loky". #if processes > 1 else "sequential",
        #                 verbose=0,  # Important to silence joblib's output
        #             )(
        #                 delayed(self._download_data_for_1_ticker_1_date)(
        #                     ticker, date_obj, timeperiod_per_file
        #                 )
        #                 for ticker, date_obj, timeperiod_per_file in list_args
        #             ),
        #             total=len(list_args),
        #             desc=f"{timeperiod_per_file} files ({ticker})",
        #             unit="files",
        #             position=1,  # Fixed position below ticker bar
        #             leave=False,  # Don't leave this bar after completion
        #         )
        #     )
        # else:
        #     print(f"nothing to do for {timeperiod_per_file} files ({ticker}")
        #     list_saved_dates = []

        if list_args:  # Only create progress bar if there are files to download
            list_saved_dates = list(
                tqdm(
                    Parallel(
                        n_jobs=processes,
                        return_as="generator",
                        backend="threading",  # Threading: faster for I/O + benchmarks work
                        verbose=0,
                    )(
                        delayed(self._download_data_for_1_ticker_1_date)(
                            ticker, date_obj, timeperiod_per_file
                        )
                        for ticker, date_obj, timeperiod_per_file in list_args
                    ),
                    total=len(list_args),
                    desc=f"{timeperiod_per_file} files ({ticker})",
                    position=1,
                    leave=True,
                )
            )
        else:
            print(f"nothing to do for {timeperiod_per_file} files ({ticker}")
            list_saved_dates = []

        #####
        list_saved_dates = [date for date in list_saved_dates if date]
        LOGGER.debug(
            "---> Downloaded %d files for ticker: %s", len(list_saved_dates), ticker
        )
        self.dict_new_points_saved_by_ticker[ticker][timeperiod_per_file] = len(
            list_saved_dates
        )

    @char
    def _download_data_for_1_ticker_1_date(
        self,
        ticker,
        date_obj,
        timeperiod_per_file="monthly",
    ):
        """Dump data for 1 ticker for 1 data"""
        # 1) Create path to file to save
        path_folder_suffix = self._get_path_suffix_to_dir_with_data(
            timeperiod_per_file, ticker
        )
        file_name = self.create_filename(
            ticker,
            date_obj,
            timeperiod_per_file=timeperiod_per_file,
            extension="zip",
        )
        str_dir_where_to_save = os.path.join(
            self.path_dir_where_to_dump, path_folder_suffix
        )
        path_zip_raw_file = os.path.join(str_dir_where_to_save, file_name)
        # 2) Create URL to file to download
        url_file_to_download = os.path.join(
            self._base_url, path_folder_suffix, file_name
        )
        # 3) Download file and unzip it
        if not self._download_raw_file(url_file_to_download, path_zip_raw_file):
            return None
        # 4) Extract zip archive
        try:
            with zipfile.ZipFile(path_zip_raw_file, "r") as zip_ref:
                zip_ref.extractall(os.path.dirname(path_zip_raw_file))
        except Exception as ex:
            LOGGER.warning(
                "Unable to unzip file %s with error: %s", path_zip_raw_file, ex
            )
            return None
        # 5) Delete the zip archive
        try:
            os.remove(path_zip_raw_file)
        except Exception as ex:
            LOGGER.warning(
                "Unable to delete zip file %s with error: %s", path_zip_raw_file, ex
            )
            return None
        return date_obj

    def _get_path_suffix_to_dir_with_data(self, timeperiod_per_file, ticker):
        """_summary_

        Args:
            timeperiod_per_file (str): Timeperiod per file: [daily, monthly]
            ticker (str): Trading pair

        Returns:
            str: suffix - https://data.binance.vision/data/ + suffix /filename
        """
        folder_path = ""
        if self._asset_class in self._FUTURES_ASSET_CLASSES:
            folder_path = os.path.join(folder_path, "futures")
        folder_path = os.path.join(folder_path, self._asset_class)
        folder_path = os.path.join(folder_path, timeperiod_per_file)
        folder_path = os.path.join(folder_path, self._data_type)
        folder_path = os.path.join(folder_path, ticker)

        if self._data_frequency:
            folder_path = os.path.join(folder_path, self._data_frequency)

        return folder_path

    def _check_circuit_breaker(self):
        """Check if circuit breaker is open, reset if enough time has passed"""
        if not self._is_circuit_broken:
            return False

        # Check if reset time has passed
        if self._circuit_breaker_last_failure:
            time_since_failure = time.time() - self._circuit_breaker_last_failure
            if time_since_failure >= self._circuit_breaker_reset_time:
                # Reset circuit breaker
                LOGGER.info(
                    "[CIRCUIT BREAKER] Reset after %d seconds. Resuming downloads.",
                    int(time_since_failure)
                )
                self._is_circuit_broken = False
                self._circuit_breaker_failures = 0
                return False

        return True

    def _record_download_failure(self, error_type):
        """Record a download failure for circuit breaker tracking"""
        self._circuit_breaker_failures += 1
        self._circuit_breaker_last_failure = time.time()

        if self._circuit_breaker_failures >= self._circuit_breaker_threshold:
            if not self._is_circuit_broken:
                self._is_circuit_broken = True
                LOGGER.error(
                    "[CIRCUIT BREAKER] Triggered after %d failures (%s). "
                    "Stopping downloads for %d seconds to avoid IP ban.",
                    self._circuit_breaker_failures,
                    error_type,
                    self._circuit_breaker_reset_time
                )

    def _record_download_success(self):
        """Record a successful download, reset failure counter"""
        if self._circuit_breaker_failures > 0:
            self._circuit_breaker_failures = 0

    def _download_raw_file(self, str_url_path_to_file, str_path_where_to_save, max_retries=3):
        """Download file from binance server by URL with retry logic and safety features

        Args:
            str_url_path_to_file: URL to download from
            str_path_where_to_save: Local path to save file
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            1 if successful, 0 if failed
        """
        # Check circuit breaker before attempting download
        if self._check_circuit_breaker():
            LOGGER.warning(
                "[CIRCUIT BREAKER] Open - skipping download: %s",
                str_url_path_to_file.split("/")[-1]
            )
            return 0

        # Track request start
        request_start = time.time()
        if self._request_start_time is None:
            self._request_start_time = request_start

        LOGGER.debug("Download file from: %s", str_url_path_to_file)
        str_url_path_to_file = str_url_path_to_file.replace("\\", "/")

        # Browser User-Agent to avoid CloudFront WAF blocking
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        for attempt in range(max_retries):
            try:
                # Track active connections
                self._active_connections += 1
                self._request_count += 1
                if self._active_connections > self._max_concurrent_connections:
                    self._max_concurrent_connections = self._active_connections

                # Log connection info every 10 requests
                if self._request_count % 10 == 0:
                    elapsed = time.time() - self._request_start_time
                    rate = self._request_count / elapsed if elapsed > 0 else 0
                    tqdm.write(
                        f"[BENCHMARK] Requests: {self._request_count} | "
                        f"Active: {self._active_connections} | Peak: {self._max_concurrent_connections} | "
                        f"Rate: {rate:.2f} req/s | Success: {self._successful_requests} | Failed: {self._failed_requests}"
                    )

                # Create request with headers
                req = urllib.request.Request(str_url_path_to_file, headers=headers)

                if "trades" not in str_url_path_to_file.lower():
                    # Simple download without progress bar
                    with urllib.request.urlopen(req, timeout=60) as response:
                        with open(str_path_where_to_save, 'wb') as out_file:
                            data = response.read()
                            out_file.write(data)
                            self._bytes_downloaded += len(data)
                else:
                    # Download with progress bar for large trades files
                    with tqdm(
                        unit="B",
                        unit_scale=True,
                        miniters=1,
                        desc="downloading: " + str_url_path_to_file.split("/")[-1],
                    ) as progress_bar:

                        def progress_hook(count, block_size, total_size):
                            current_size = block_size * count
                            if total_size > 0:
                                previous_progress = progress_bar.n / total_size * 100
                                current_progress = current_size / total_size * 100
                                if current_progress > previous_progress + 10:
                                    progress_bar.total = total_size
                                    progress_bar.update(current_size - progress_bar.n)

                        # Use urlretrieve with custom opener that includes headers
                        opener = urllib.request.build_opener()
                        opener.addheaders = list(headers.items())
                        urllib.request.install_opener(opener)
                        urllib.request.urlretrieve(
                            str_url_path_to_file, str_path_where_to_save, progress_hook
                        )
                        # Track bytes downloaded
                        import os as _os_size
                        if _os_size.path.exists(str_path_where_to_save):
                            self._bytes_downloaded += _os_size.path.getsize(str_path_where_to_save)

                # Success - reset failure counter and track stats
                self._record_download_success()
                self._successful_requests += 1
                self._active_connections -= 1

                # Track request duration
                request_duration = time.time() - request_start
                self._request_timings.append(request_duration)

                return 1

            except urllib.error.HTTPError as ex:
                self._active_connections -= 1
                if ex.code == 404:
                    # File doesn't exist - don't retry, don't count as failure
                    self._not_found_requests += 1
                    LOGGER.debug("[WARNING] File not found (404): %s", str_url_path_to_file)
                    return 0
                elif ex.code == 418:
                    # IP BAN - Critical failure, trigger circuit breaker immediately
                    self._record_download_failure("IP_BAN_418")
                    self._failed_requests += 1
                    LOGGER.error(
                        "[IP BAN] HTTP 418 - IP banned by Binance. All downloads stopped. "
                        "Ban typically lasts 2 minutes to 3 days. Wait before retrying."
                    )
                    # Force circuit breaker open
                    self._is_circuit_broken = True
                    self._circuit_breaker_failures = self._circuit_breaker_threshold
                    return 0
                elif ex.code == 403:
                    # Forbidden - WAF rate limiting
                    self._record_download_failure("WAF_403")
                    wait_time = (2 ** attempt) * 5  # Exponential backoff: 5s, 10s, 20s
                    LOGGER.warning(
                        "[RATE LIMIT] 403 Forbidden (WAF block) on %s. Waiting %ds before retry %d/%d",
                        str_url_path_to_file.split("/")[-1],
                        wait_time,
                        attempt + 1,
                        max_retries
                    )
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        LOGGER.error("Max retries reached for: %s", str_url_path_to_file)
                        self._failed_requests += 1
                        return 0
                elif ex.code == 429:
                    # Too Many Requests - Standard rate limiting
                    self._record_download_failure("RATE_LIMIT_429")
                    wait_time = (2 ** attempt) * 5
                    LOGGER.warning(
                        "[RATE LIMIT] 429 Too Many Requests on %s. Waiting %ds before retry %d/%d",
                        str_url_path_to_file.split("/")[-1],
                        wait_time,
                        attempt + 1,
                        max_retries
                    )
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                        continue
                    self._failed_requests += 1
                    return 0
                elif ex.code == 503:
                    # Service Unavailable - CloudFront overload
                    self._record_download_failure("CLOUDFRONT_503")
                    wait_time = (2 ** attempt) * 5
                    LOGGER.warning(
                        "[CLOUDFRONT] 503 Service Unavailable (CDN overload) on %s. Waiting %ds",
                        str_url_path_to_file.split("/")[-1],
                        wait_time
                    )
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                        continue
                    self._failed_requests += 1
                    return 0
                else:
                    LOGGER.warning("HTTP Error %s: %s", ex.code, str_url_path_to_file)
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    self._failed_requests += 1
                    return 0

            except urllib.error.URLError as ex:
                self._active_connections -= 1
                # URLError often wraps SSL errors which indicate rate limiting
                error_str = str(ex.reason).lower()
                if "ssl" in error_str or "eof" in error_str or "protocol" in error_str:
                    # SSL errors are actually rate limiting per Binance support
                    self._record_download_failure("SSL_ERROR_RATE_LIMIT")
                    wait_time = (2 ** attempt) * 5
                    LOGGER.warning(
                        "[RATE LIMIT] SSL/Connection error (hidden rate limit) on %s. "
                        "Waiting %ds before retry %d/%d",
                        str_url_path_to_file.split("/")[-1],
                        wait_time,
                        attempt + 1,
                        max_retries
                    )
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                        continue
                    self._failed_requests += 1
                    return 0
                else:
                    LOGGER.warning("URL Error: %s - %s", ex.reason, str_url_path_to_file)
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    self._failed_requests += 1
                    return 0

            except Exception as ex:
                self._active_connections -= 1
                # Catch-all for other errors (connection resets, etc)
                error_str = str(ex).lower()
                if "ssl" in error_str or "connection" in error_str or "reset" in error_str:
                    # Connection issues often indicate rate limiting
                    self._record_download_failure("CONNECTION_ERROR")
                    LOGGER.warning(
                        "[CONNECTION] Connection error (possible rate limit) on %s: %s",
                        str_url_path_to_file.split("/")[-1],
                        ex
                    )
                else:
                    LOGGER.warning("Unable to download raw file: %s - %s", ex, str_url_path_to_file)

                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                self._failed_requests += 1
                return 0

        self._failed_requests += 1
        return 0  # All retries exhausted

    def _print_dump_statistics(self, print_benchmark=True):
        """Print the latest dump statistics

        Args:
            print_benchmark (bool): Whether to print request benchmark statistics.
                Defaults to True. Set to False when processing multiple tickers
                to avoid repetitive output.
        """
        # Print request benchmark statistics (if enabled)
        if print_benchmark:
            self._print_request_benchmark()

        LOGGER.info(
            "Tried to dump data for %d tickers:",
            len(self.dict_new_points_saved_by_ticker),
        )
        if len(self.dict_new_points_saved_by_ticker) < 50:
            self._print_full_dump_statististics()
        else:
            self._print_short_dump_statististics()

    def _print_request_benchmark(self):
        """Print detailed request benchmarking statistics"""
        if self._request_count == 0:
            return

        print("\n" + "=" * 79)
        print("REQUEST BENCHMARK STATISTICS")
        print("=" * 79)

        total_time = time.time() - self._request_start_time if self._request_start_time else 0
        avg_rate = self._request_count / total_time if total_time > 0 else 0

        success_pct = (self._successful_requests / self._request_count * 100) if self._request_count > 0 else 0
        failed_pct = (self._failed_requests / self._request_count * 100) if self._request_count > 0 else 0
        not_found_pct = (self._not_found_requests / self._request_count * 100) if self._request_count > 0 else 0

        print(f"Total Requests Sent: {self._request_count}")
        print(f"---> Successful: {self._successful_requests} ({success_pct:.1f}%)")
        print(f"---> Not Found (404): {self._not_found_requests} ({not_found_pct:.1f}%)")
        print(f"---> Failed (Rate Limit/Error): {self._failed_requests} ({failed_pct:.1f}%)")

        print("Concurrent Connections:")
        print(f"---> Peak Concurrent: {self._max_concurrent_connections}")
        print(f"---> Configured Max: {self._max_concurrent_downloads}")

        print("Request Rate:")
        print(f"---> Average: {avg_rate:.2f} requests/second")
        print(f"---> Total Duration: {total_time:.1f} seconds")

        if self._request_timings:
            avg_duration = sum(self._request_timings) / len(self._request_timings)
            min_duration = min(self._request_timings)
            max_duration = max(self._request_timings)
            print("Request Duration:")
            print(f"---> Average: {avg_duration:.2f} seconds")
            print(f"---> Min: {min_duration:.2f} seconds")
            print(f"---> Max: {max_duration:.2f} seconds")

        if self._bytes_downloaded > 0:
            mb_downloaded = self._bytes_downloaded / (1024 * 1024)
            speed_mbps = (mb_downloaded * 8) / total_time if total_time > 0 else 0
            print("Data Transfer:")
            print(f"---> Total Downloaded: {mb_downloaded:.2f} MB")
            print(f"---> Average Speed: {speed_mbps:.2f} Mbit/s")

        print("=" * 79 + "\n")

    def print_final_benchmark(self):
        """Print final cumulative benchmark statistics across all dumps.

        Call this method at the end of your script after processing multiple tickers
        to see the total cumulative statistics. The benchmark counters accumulate
        across all dump_data() calls since the BinanceDataDumper was created.

        Example:
            # Process multiple tickers without per-ticker benchmark spam
            for ticker in tickers:
                dumper.dump_data(tickers=[ticker], print_benchmark=False)

            # Print final cumulative stats at the end
            dumper.print_final_benchmark()
        """
        self._print_request_benchmark()

    def _print_full_dump_statististics(self):
        """"""
        for ticker in self.dict_new_points_saved_by_ticker:
            dict_stats = self.dict_new_points_saved_by_ticker[ticker]
            LOGGER.info(
                "---> For %s new data saved for: %d months %d days",
                ticker,
                dict_stats.get("monthly", 0),
                dict_stats.get("daily", 0),
            )

    def _print_short_dump_statististics(self):
        """"""
        # Gather stats
        int_non_empty_dump_res = 0
        int_empty_dump_res = 0
        list_months_saved = []
        list_days_saved = []
        for ticker in self.dict_new_points_saved_by_ticker:
            dict_stats = self.dict_new_points_saved_by_ticker[ticker]
            list_months_saved.append(dict_stats.get("monthly", 0))
            list_days_saved.append(dict_stats.get("daily", 0))
            if dict_stats["monthly"] or dict_stats["daily"]:
                int_non_empty_dump_res += 1
            else:
                int_empty_dump_res += 1
        #####
        # Print Stats
        LOGGER.info("---> General stats:")
        LOGGER.info(
            "------> NEW Data WAS dumped for %d trading pairs", int_non_empty_dump_res
        )
        LOGGER.info(
            "------> NEW Data WASN'T dumped for %d trading pairs", int_empty_dump_res
        )
        #####
        LOGGER.info("---> New months saved:")
        counter_months = Counter(list_months_saved)
        for value, times in counter_months.most_common(5):
            LOGGER.info("------> For %d tickers saved: %s months", times, value)
        if len(counter_months) > 5:
            LOGGER.info("------> ...")
        LOGGER.info("---> New days saved:")
        counter_days = Counter(list_days_saved)
        for value, times in counter_days.most_common(5):
            LOGGER.info("------> For %d tickers saved: %s days", times, value)
        if len(counter_days) > 5:
            LOGGER.info("------> ...")
        LOGGER.info("=" * 79)

    def _get_list_trading_pairs_to_download(
        self, tickers=None, tickers_to_exclude=None
    ):
        """
        Create list of tickers for which to get data (by default all **USDT)
        """
        all_tickers = self.get_list_all_trading_pairs()
        LOGGER.info("---> Found overall tickers: %d", len(all_tickers))

        if tickers:
            LOGGER.info("---> Filter to asked tickers: %d", len(tickers))
            tickers_to_use = [ticker for ticker in all_tickers if ticker in tickers]
        else:
            LOGGER.info("---> Filter to USDT tickers")
            tickers_to_use = [
                ticker for ticker in all_tickers if ticker.endswith("USDT")
            ]
        LOGGER.info("------> Tickers left: %d", len(tickers_to_use))
        #####
        if tickers_to_exclude:
            LOGGER.info("---> Exclude the asked tickers: %d", len(tickers_to_exclude))
            tickers_to_use = [
                ticker for ticker in tickers_to_use if ticker not in tickers_to_exclude
            ]
            LOGGER.info("------> Tickers left: %d", len(tickers_to_use))
        return tickers_to_use

    @staticmethod
    def _create_list_dates_for_timeperiod(
        date_start,
        date_end=None,
        timeperiod_per_file="monthly",
    ):
        """Create list dates with asked frequency for [date_start, date_end]"""
        list_dates = []
        if date_end is None:
            date_end = datetime.datetime.utcnow().date
        LOGGER.debug("Create dates to dump data for: %s -> %s", date_start, date_end)
        #####
        date_to_use = date_start
        while date_to_use <= date_end:
            list_dates.append(date_to_use)
            if timeperiod_per_file == "monthly":
                date_to_use = date_to_use + relativedelta(months=1)
            else:
                date_to_use = date_to_use + relativedelta(days=1)
        LOGGER.debug("---> Dates created: %d", len(list_dates))
        return list_dates


# class ParallelBinanceDataDumper:
#     def __init__(self, base_dumper):
#         """Initialize with an instance of BinanceDataDumper"""
#         self.dumper = base_dumper
#         self.logger = logging.getLogger(__name__)

#     def _process_single_ticker(
#         self, ticker, date_start, date_end, is_to_update_existing
#     ):
#         """Process a single ticker's data download"""
#         try:
#             self.dumper._download_data_for_1_ticker(
#                 ticker=ticker,
#                 date_start=date_start,
#                 date_end=date_end,
#                 timeperiod_per_file="monthly",
#                 is_to_update_existing=is_to_update_existing,
#             )

#             # Handle daily data for the last month
#             if date_end:
#                 date_end_first_day = datetime.date(date_end.year, date_end.month, 1)
#                 self.dumper._download_data_for_1_ticker(
#                     ticker=ticker,
#                     date_start=date_end_first_day,
#                     date_end=date_end,
#                     timeperiod_per_file="daily",
#                     is_to_update_existing=is_to_update_existing,
#                 )
#             return ticker, True
#         except Exception as e:
#             self.logger.error(f"Error processing ticker {ticker}: {str(e)}")
#             return ticker, False

#     def parallel_dump_data(
#         self,
#         tickers,
#         date_start=None,
#         date_end=None,
#         is_to_update_existing=False,
#         n_jobs=-1,
#         tickers_to_exclude=None,
#     ):
#         """
#         Parallel version of dump_data that processes multiple tickers simultaneously

#         Args:
#             tickers (list[str]): List of trading pairs to dump data for
#             date_start (datetime.date): Start date for data dump
#             date_end (datetime.date): End date for data dump
#             is_to_update_existing (bool): Whether to update existing data
#             n_jobs (int): Number of parallel jobs (default: -1 uses all cores)
#             tickers_to_exclude (list[str]): Tickers to exclude from processing
#         """
#         # Handle date defaults
#         if date_start is None:
#             date_start = datetime.date(year=2017, month=1, day=1)
#         if date_end is None:
#             date_end = datetime.utcnow().date() - relativedelta(days=1)

#         # Filter tickers
#         if tickers is None:
#             tickers = [
#                 t
#                 for t in self.dumper.get_list_all_trading_pairs()
#                 if t.endswith("USDT")
#             ]
#         if tickers_to_exclude:
#             tickers = [t for t in tickers if t not in tickers_to_exclude]

#         self.logger.info(f"Starting parallel download for {len(tickers)} tickers")

#         # Create progress bar for overall process
#         with tqdm(total=len(tickers), desc="Processing Tickers", position=0) as pbar:
#             results = Parallel(n_jobs=n_jobs, backend="loky", verbose=0)(
#                 delayed(self._process_single_ticker)(
#                     ticker, date_start, date_end, is_to_update_existing
#                 )
#                 for ticker in tickers
#             )

#             # Update progress bar after each ticker completes
#             for _ in results:
#                 pbar.update(1)

#         # Process results
#         successful = [ticker for ticker, success in results if success]
#         failed = [ticker for ticker, success in results if not success]

#         self.logger.info(
#             f"Download complete. Successful: {len(successful)}, Failed: {len(failed)}"
#         )
#         if failed:
#             self.logger.warning(f"Failed tickers: {', '.join(failed)}")

#         return successful, failed


# class ParallelBinanceDataDumper:
#     """POINTLESS AS EVEN 1 Dumper uses my Internetconnection fully! 330 MBIT!!"""

#     def __init__(self, base_dumper):
#         self.dumper = base_dumper

#     def parallel_dump(self, tickers, n_jobs=-1, **kwargs):
#         """Parallel download for multiple tickers

#         Args:
#             tickers (list[str]): List of tickers to process in parallel
#             n_jobs (int): Number of parallel jobs
#             **kwargs: All other parameters passed directly to dump_data
#         """
#         with tqdm(total=len(tickers), desc="Processing Tickers", unit="ticker") as pbar:
#             Parallel(n_jobs=n_jobs)(
#                 delayed(self.dumper.dump_data)(
#                     tickers=[ticker],  # Single ticker per job
#                     **kwargs,  # Pass through all other parameters
#                 )
#                 for ticker in tickers
#             )
#             pbar.update(len(tickers))


class ParallelBinanceDataDumper:
    """POINTLESS AS EVEN 1 Dumper uses my Internetconnection fully! 330 MBIT!!"""

    def __init__(self, base_dumper):
        self.dumper = base_dumper

    def parallel_dump(self, tickers, n_jobs=-1, **kwargs):
        """Parallel download for multiple tickers

        Args:
            tickers (list[str]): List of tickers to process in parallel
            n_jobs (int): Number of parallel jobs
            **kwargs: All other parameters passed directly to dump_data
        """
        with tqdm(total=len(tickers), desc="Processing Tickers", unit="ticker") as pbar:
            # Use return_as="generator" to get results as they are completed
            results_generator = Parallel(n_jobs=n_jobs, return_as="generator")(
                delayed(self.dumper.dump_data)(
                    tickers=[ticker],  # Single ticker per job
                    **kwargs,  # Pass through all other parameters
                )
                for ticker in tickers
            )
            # Iterate over the generator and update the progress bar as each job completes
            for _ in results_generator:
                pbar.update(1)
