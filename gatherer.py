"""
Data Gathering Functions
"""

import csv
import datetime
import gzip
import io
import json
import os
import pathlib
import requests
import tqdm
import zipfile

import configs as cfg


def get_all_dates(
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        ) -> list:  # ['2022-02-24', '2022-02-25']
    """Lists all dates (YYYY-MM-DD) from start to yesterday"""
    assert interval in cfg.SUPPORTED_INTERVALS, f'interval type {interval} not supported'
    date_i = datetime.date.fromisoformat(cfg.HISTORY_START_DATE)
    today = datetime.datetime.now(datetime.timezone.utc).date()
    all_days_list = []
    while date_i < today:
        all_days_list.append(str(date_i))
        date_i += datetime.timedelta(days=1)
    return all_days_list


def get_history_dates(
        tpair: str,  # 'ETHUSDT'
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        ) -> list:  # ['2022-01-30', '2022-01-31']
    """Gets all dates found in local data for the chosen trading pair"""
    assert interval in cfg.SUPPORTED_INTERVALS, f'interval type {interval} not supported'
    path = cfg.PAIRS_DATA_DIRTEMPLATE.format(trade_pair=tpair, interval=interval)
    data_files = [str(p) for p in pathlib.Path(path).rglob('*jl.gz')]
    found_dates = []
    for file in data_files:
        found_dates.extend([json.loads(r)['dt'] for r in gzip.open(file, 'r')])
    return found_dates


def filter_missing_data(
        local_dates: list,  # ['2022-01-30', '2022-01-31']
        full_dates: list,  # ['2022-02-24', '2022-02-25']
        ) -> tuple:  # (['2020-01', '2020-02'], ['2022-02-01', '2022-02-02'])
    """Gets missing months and days from local data"""
    missing_dates = sorted(list(set(full_dates) - set(local_dates)))
    # generating grouped dates by month
    months = sorted(set([d[:7] for d in full_dates]))
    months_days = {m: sorted([d for d in full_dates if d[:7] == m]) for m in months}
    this_month = str(datetime.datetime.now(datetime.timezone.utc).date())[:7]
    # collecting grouped dates
    missing_months = []
    missing_days = []
    for month, days in months_days.items():
        if month == this_month:
            missing_days.extend([d for d in days if d in missing_dates])
        elif all([d in missing_dates for d in days]):
            missing_months.append(month)
        else:
            missing_days.extend(sorted([d for d in days if d in missing_dates]))
    return missing_months, missing_days


def get_binance_data(
        trade_pair: str,  # 'ETHUSDT'
        dt: str,  # '2022-01' or '2022-01-01'
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        ) -> list:  # [['16383','2.3','2.4','163840','0']]
    """Gets daily data for the trading pair"""
    # checking if data is montly or daily
    assert len(dt) in [7, 10], f'unexpected date format: {dt}'
    freq = 'monthly' if len(dt) == 7 else 'daily'
    # preparing url
    base_url = 'https://data.binance.vision'
    query_url = ('{base_url}/data/spot/{freq}/klines/{trade_pair}/'
                 '{interval}/{trade_pair}-{interval}-{date}.zip')
    final_url = query_url.format(
                            base_url=base_url,
                            freq=freq,
                            trade_pair=trade_pair,
                            interval=interval,
                            date=dt,
                            )
    # more info at https://github.com/binance/binance-public-data
    res = requests.get(final_url)
    if res.status_code != 200:
        print(f'file not found: {final_url}')
        return []
    # reading file contents
    zip_file = zipfile.ZipFile(io.BytesIO(res.content))
    with zip_file.open(zip_file.namelist()[0], 'r') as csv_file:
        with io.TextIOWrapper(csv_file) as csv_content:
            raw_rows = list(csv.reader(csv_content))
    return raw_rows


def parse_binance_data(
        csv_rows: list,  # [['16383','2.3','2.4','163840','0']]
        ) -> list:  # [{'open_time': 1638316800.0,'open': 26290.78,'dt': '2021-12-01'}]
    """Parses list of results into dicts"""
    # naming fields
    rows = [[float(v) for v in r] for r in csv_rows]
    csv_cols = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                'volume', 'trades', 'taker_base_volume', 'taker_quote_volume', 'ignore']
    rows = [{k:v for k,v in zip(csv_cols, r)} for r in rows]
    # removing fields
    ignore_cols = ['taker_base_volume', 'taker_quote_volume', 'ignore']
    rows = [{k: v for k,v in r.items() if k not in ignore_cols} for r in rows]
    # int fields
    int_cols = ['trades']
    rows = [{k: (int(v) if k in int_cols else v) for k,v in r.items()} for r in rows]
    # datetime fields
    dttm_cols = ['open_time', 'close_time']
    for r in rows:
        for c in dttm_cols:
            r[c] = r[c] / 1000
            r[f'{c}_iso'] = str(datetime.datetime.utcfromtimestamp(r[c]))[:19]
        r['dt'] = r['open_time_iso'][:10]
    # sorting
    rows = sorted(rows, key=lambda x: x['open_time'])
    return rows


def store_data(
        data_rows: list,  # [{'open_time': 1638316800.0,'open': 26290.78,'dt': '2021-12-01'}]
        year_month: str,  # '2021-12'
        trade_pair: str,  # 'ETHUSDT'
        override: bool = False,  # True
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        ) -> int:  # 31
    """Generates compressed jsonlines file with data"""
    data_path = cfg.PAIRS_DATA_DIRTEMPLATE.format(trade_pair=trade_pair, interval=interval)
    pathlib.Path(data_path).mkdir(parents=True, exist_ok=True)
    fname = cfg.PRICES_FILETEMPLATE.format(
                                    trade_pair=trade_pair,
                                    interval=interval,
                                    yearmonth=year_month.replace('-', ''),
                                    )
    f_path_full = f'{data_path}/{fname}'
    if os.path.isfile(f_path_full):
        if not override:
            current_data = [json.loads(r) for r in gzip.open(f_path_full, 'r')]
            data_rows.extend(current_data)
            data_rows = sorted(data_rows, key=lambda x: x['open_time'])
        os.remove(f_path_full)
    rows_num = 0
    with gzip.open(f_path_full, 'wt') as f_out:
        for row in data_rows:
            f_out.write(''.join([json.dumps(row), '\n']))
            rows_num += 1
    return rows_num


def download_missing_data(
        trade_pair: str,  # 'ETHUSDT'
        months_list: list,  # ['2021-12', '2022-01']
        days_list: list,  # ['2022-03-01', '2022-03-02']
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        pbar: bool = True,  # False
        ) -> tuple:  # (61, ['2021-12'])
    """Downloads data to disk for each month of target trading pair"""
    # stating returned variables
    rows_written = 0
    failed_downloads = []
    # mixing months and lists of same month days
    months_from_days = sorted(set([x[:7] for x in days_list]))
    grouped_days = [sorted([y for y in days_list if y[:7] == m]) for m in months_from_days]
    dates_list = sorted(months_list) + grouped_days
    # progress bar
    if pbar:
        dates_list = tqdm.tqdm(dates_list, ncols=cfg.TQDM_NCOLS)
    # iterating
    for dt in dates_list:
        # collecting data
        if isinstance(dt, str):
            ym = dt
            data_rows = get_binance_data(trade_pair, dt, interval)
            if len(data_rows) == 0:
                failed_downloads.append(dt)
                continue
        elif isinstance(dt, list):
            ym = dt[0][:7]
            data_rows = []
            for d in dt:
                res = get_binance_data(trade_pair, d, interval)
                if len(res) == 0:
                    failed_downloads.append(d)
                else:
                    data_rows.extend(res)
            if len(data_rows) == 0:
                continue
        # parsing data
        parsed_rows = parse_binance_data(data_rows)
        # storing file
        rows_written += store_data(
                            data_rows=parsed_rows,
                            year_month=ym,
                            trade_pair=trade_pair,
                            interval=interval,
                            )
    return (rows_written, failed_downloads)


def update_ticker_data(
        target_pair: str,  # 'ETHUSDT'
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        ) -> None:
    """Adds missing data of trading pair to local storage"""
    all_dates = get_all_dates(interval)
    found_dates = get_history_dates(target_pair, interval)
    miss_months, miss_days = filter_missing_data(found_dates, all_dates)
    success_rows, failed_dts = download_missing_data(target_pair, miss_months, miss_days, interval)
    failed_print = '' if len(failed_dts) == 0 else failed_dts
    print(f'{success_rows} new rows of data obtained, {len(failed_dts)} operations failed {failed_print}')
    return
