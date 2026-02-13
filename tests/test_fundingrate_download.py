"""
Test script to verify fundingRate download functionality
Downloads funding rate data for a few perpetual futures symbols
"""
import os
import tempfile
import shutil
from datetime import date
from binance_historical_data import BinanceDataDumper


def test_fundingrate_download():
    """Test downloading fundingRate data for perpetual futures"""

    # Create temporary directory for test data
    test_dir = tempfile.mkdtemp(prefix="fundingrate_test_")

    try:
        print("="*70)
        print("FUNDINGRATE DOWNLOAD TEST")
        print("="*70)
        print(f"Test directory: {test_dir}")
        print()

        # Test symbols (popular perpetual futures)
        test_symbols = ["BTCUSDT", "ETHUSDT"]

        # Short date range for testing (just a few months)
        start_date = date(2024, 1, 1)
        end_date = date(2024, 3, 31)

        print(f"Downloading fundingRate for: {test_symbols}")
        print(f"Date range: {start_date} to {end_date}")
        print(f"Asset class: um (USD-margined perpetual futures)")
        print()

        # Create dumper for fundingRate
        dumper = BinanceDataDumper(
            path_dir_where_to_dump=test_dir,
            asset_class="um",  # USD-margined futures
            data_type="fundingRate",
            # No data_frequency needed - fundingRate doesn't use it!
            max_concurrent_downloads=3,
        )

        # Download data
        print("Starting download...")
        print("-"*70)
        dumper.dump_data(
            tickers=test_symbols,
            date_start=start_date,
            date_end=end_date,
            is_to_update_existing=False,
            print_benchmark=True,
        )

        # Verify downloaded files
        print()
        print("="*70)
        print("VERIFICATION")
        print("="*70)

        all_files_found = True
        for symbol in test_symbols:
            monthly_path = os.path.join(test_dir, "futures", "um", "monthly", "fundingRate", symbol)
            daily_path = os.path.join(test_dir, "futures", "um", "daily", "fundingRate", symbol)

            monthly_files = []
            daily_files = []

            if os.path.exists(monthly_path):
                monthly_files = [f for f in os.listdir(monthly_path) if f.endswith('.csv')]

            if os.path.exists(daily_path):
                daily_files = [f for f in os.listdir(daily_path) if f.endswith('.csv')]

            print(f"\n{symbol}:")
            print(f"  Monthly files: {len(monthly_files)}")
            if monthly_files:
                for f in sorted(monthly_files)[:3]:  # Show first 3
                    file_path = os.path.join(monthly_path, f)
                    size_kb = os.path.getsize(file_path) / 1024
                    print(f"    - {f} ({size_kb:.1f} KB)")
                if len(monthly_files) > 3:
                    print(f"    ... and {len(monthly_files) - 3} more")

            print(f"  Daily files: {len(daily_files)}")
            if daily_files:
                for f in sorted(daily_files)[:3]:  # Show first 3
                    file_path = os.path.join(daily_path, f)
                    size_kb = os.path.getsize(file_path) / 1024
                    print(f"    - {f} ({size_kb:.1f} KB)")
                if len(daily_files) > 3:
                    print(f"    ... and {len(daily_files) - 3} more")

            if len(monthly_files) == 0 and len(daily_files) == 0:
                print(f"  [WARNING] No files found for {symbol}")
                all_files_found = False

        print()
        print("="*70)
        if all_files_found:
            print("[SUCCESS] fundingRate download working correctly!")
        else:
            print("[WARNING] Some symbols had no files (might not have existed in date range)")
        print("="*70)

        # Show example file path structure
        print()
        print("File structure:")
        print(f"  {test_dir}/")
        print(f"    └── futures/")
        print(f"        └── um/")
        print(f"            ├── monthly/fundingRate/[SYMBOL]/[SYMBOL]-fundingRate-YYYY-MM.csv")
        print(f"            └── daily/fundingRate/[SYMBOL]/[SYMBOL]-fundingRate-YYYY-MM-DD.csv")
        print()

    finally:
        # Clean up test directory
        print(f"Cleaning up test directory: {test_dir}")
        shutil.rmtree(test_dir, ignore_errors=True)
        print("Done!")


if __name__ == "__main__":
    test_fundingrate_download()
