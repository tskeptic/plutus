"""
Data Gathering Functions
"""

import collections
import csv
import datetime
import io
import os
import pathlib
import requests
import tqdm
import zipfile

import configs as cfg
import utils


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
        docs = utils.read_jsonlines_file(file)
        found_dates.extend([r['dt'] for r in docs])
    return sorted(found_dates)


def filter_missing_data(
        trade_pair: str,  # 'ETHUSDT'
        local_dates: list,  # ['2022-01-30', '2022-01-31']
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        month_proxy: int = 7,  # 14
        ) -> tuple:  # (['2020-01', '2020-02'], ['2022-02-01', '2022-02-02'])
    """Gets missing months and days from local data"""
    assert interval in cfg.SUPPORTED_INTERVALS, f'interval type {interval} not supported'
    full_dates = get_all_dates(interval)
    missing_dates_prev = sorted(list(set(full_dates) - set(local_dates)))
    # checking metadata
    pair_meta = get_availability_metadata(trade_pair, interval)
    if pair_meta:
        missing_dates = missing_dates_prev.copy()
        for dt_range in pair_meta['unavailable_periods']:
            for dt in missing_dates_prev:
                if dt_range[0] <= dt <= dt_range[1]:
                    missing_dates.remove(dt)
    else:
        missing_dates = missing_dates_prev
    # generating grouped dates by month
    months = sorted(set([d[:7] for d in full_dates]))
    months_days = {m: sorted([d for d in full_dates if d[:7] == m]) for m in months}
    today = datetime.datetime.now(datetime.timezone.utc)
    this_month = str(today.date())[:7]
    close_month = str((today - datetime.timedelta(days=month_proxy)).date())[:7]
    # collecting grouped dates
    missing_months = []
    missing_days = []
    for month, days in months_days.items():
        if month in [this_month, close_month]:
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
        ) -> tuple:  # 200, [['16383','2.3','2.4','163840','0']]
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
        return res.status_code, []
    # reading file contents
    zip_file = zipfile.ZipFile(io.BytesIO(res.content))
    with zip_file.open(zip_file.namelist()[0], 'r') as csv_file:
        with io.TextIOWrapper(csv_file) as csv_content:
            raw_rows = list(csv.reader(csv_content))
    return res.status_code, raw_rows


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
    # file name
    data_path = cfg.PAIRS_DATA_DIRTEMPLATE.format(trade_pair=trade_pair, interval=interval)
    pathlib.Path(data_path).mkdir(parents=True, exist_ok=True)
    fname = cfg.PRICES_FILETEMPLATE.format(
                                    trade_pair=trade_pair,
                                    interval=interval,
                                    yearmonth=year_month.replace('-', ''),
                                    )
    f_path_full = f'{data_path}/{fname}'
    # checking if exists
    if os.path.isfile(f_path_full):
        if not override:
            current_data = utils.read_jsonlines_file(f_path_full)
            data_rows.extend(current_data)
            data_rows = sorted(data_rows, key=lambda x: x['open_time'])
    # writing
    rows_num = utils.write_jsonlines_file(f_path_full, data_rows)
    return rows_num


def get_seqs(
        dt_seq: list,  # ['2021-04', '2021-05-01', '2021-05-02']
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        ) -> list:  # [['2020-01-01', '2021-05-27']]
    """Gets interval of sequential dates without data"""
    assert interval in cfg.SUPPORTED_INTERVALS, f'interval type {interval} not supported'
    # prep full lists as referenc
    all_days = get_all_dates(interval)
    # prep seq (transf months into days)
    new_seq = [[d for d in all_days if d[:7]==m] if len(m)==7 else [m] for m in dt_seq]
    new_seq = sorted([i for subl in new_seq for i in subl])
    # iterating
    res_seqs = []
    start = new_seq[0]
    for idt in range(1, len(new_seq)):
        # comparisons variables
        current_dt = new_seq[idt]
        prev_dt = new_seq[idt-1]
        prev_theor = all_days[all_days.index(current_dt)-1]
        # blank space inbetween (blank -> data found)
        if prev_theor != prev_dt:
            res_seqs.append([start, prev_dt])
            start = current_dt
        # closing last seq
        if len(new_seq) == (idt+1):
            res_seqs.append([start, current_dt])
    return res_seqs


def get_availability_metadata(
        trade_pair: str,  # 'ETHUSDT'
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        ) -> dict:  # {"trade_pair": "MA", "interval": "1d", "unavailable_periods": [["2020-01-01", "2021-04-30"]]}
    """Gets availability metadata about trading pair"""
    # reading current availability metadata
    ameta_file = f'{cfg.PAIRS_DIR}/{cfg.PAIRS_AVAILABILITY_METADATA}'
    avail_metas = utils.read_jsonlines_file(ameta_file)
    # searching for this metadata
    dcheck = lambda d: (d.get('trade_pair') == trade_pair) and (d.get('interval') == interval)
    this_meta = [x for x in avail_metas if dcheck(x)]
    assert len(this_meta) < 2, f'duplicated avail. metadata for pair {trade_pair}'
    return this_meta[0] if this_meta else None


def register_unavailability(
        trade_pair: str,  # 'ETHUSDT'
        tries_404s: list,  # ['2022-02', '2022-03-01', '2022-03-02']
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        age_disconsider: int = 14,  # 7
        ) -> None:
    """Register unavailable periods locally"""
    assert interval in cfg.SUPPORTED_INTERVALS, f'interval type {interval} not supported'
    # filtering out recent dates
    today = datetime.datetime.now(datetime.timezone.utc).date()
    date_limit = str(today - datetime.timedelta(days=age_disconsider))
    filtered_fs = [x for x in tries_404s if x < date_limit[:len(x)]]
    if len(filtered_fs) == 0:
        return None
    # getting seq periods
    unav_periods = get_seqs(filtered_fs, interval)
    # this pair metadata
    this_meta = get_availability_metadata(trade_pair, interval)
    # reading current availability metadata
    ameta_file = f'{cfg.PAIRS_DIR}/{cfg.PAIRS_AVAILABILITY_METADATA}'
    avail_metadata = utils.read_jsonlines_file(ameta_file)
    others_meta = [x for x in avail_metadata if x != this_meta]
    # upserting info
    if this_meta:  # updating
        new_meta = this_meta
        new_meta['unavailable_periods'].extend(unav_periods)
        new_meta['unavailable_periods'] = sorted(new_meta['unavailable_periods'], key=lambda x: x[0])
    else:  # creating
        new_meta = {
            'trade_pair': trade_pair,
            'interval': interval,
            'unavailable_periods': unav_periods,
            }
    # storing new obj
    metas_list = others_meta + [new_meta]
    metas_list = sorted(metas_list, key=lambda x: x['trade_pair'])
    n = utils.write_jsonlines_file(ameta_file, metas_list)
    # TODO: change prints to logs
    print(f'unavailability registered for {trade_pair} : {unav_periods}')
    return None


def download_missing_data(
        trade_pair: str,  # 'ETHUSDT'
        months_list: list,  # ['2021-12', '2022-01']
        days_list: list,  # ['2022-03-01', '2022-03-02']
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        pbar: bool = True,  # False
        ) -> tuple:  # 61, {404: ['2021-12']}
    """Downloads data to disk for each month of target trading pair"""
    # stating returned variables
    rows_obtained = 0
    failed_downloads = collections.defaultdict(list)
    # mixing months and lists of same month days
    months_from_days = sorted(set([x[:7] for x in days_list]))
    grouped_days = [sorted([y for y in days_list if y[:7] == m]) for m in months_from_days]
    dates_list = sorted(months_list) + grouped_days
    # progress bar
    if pbar:
        flatten_list = lambda li: [i for subl in li for i in flatten_list(subl)] if type(li) is list else [li]
        pbar_total = len(flatten_list(dates_list))
        pbar = tqdm.tqdm(total=pbar_total, ncols=cfg.TQDM_NCOLS)
    # iterating
    for dt in dates_list:
        # collecting data
        if isinstance(dt, str):
            ym = dt
            status, data_rows = get_binance_data(trade_pair, dt, interval)
            if pbar:
                pbar.update(1)
            if status != 200:
                failed_downloads[status].append(dt)
                continue
        elif isinstance(dt, list):
            ym = dt[0][:7]
            data_rows = []
            for d in dt:
                status, res = get_binance_data(trade_pair, d, interval)
                if pbar:
                    pbar.update(1)
                if status != 200:
                    failed_downloads[status].append(d)
                else:
                    data_rows.extend(res)
            if len(data_rows) == 0:
                continue
        # parsing data
        parsed_rows = parse_binance_data(data_rows)
        rows_obtained += len(parsed_rows)
        # storing file
        _ = store_data(
                data_rows=parsed_rows,
                year_month=ym,
                trade_pair=trade_pair,
                interval=interval,
                )
    if pbar:
        pbar.close()
    # storing unavailable periods
    errors_found = len(failed_downloads[404]) > 0
    api_working = rows_obtained > 0
    if errors_found and api_working:
        register_unavailability(trade_pair, failed_downloads[404], interval)
    return (rows_obtained, failed_downloads)


def update_ticker_data(
        target_pair: str,  # 'ETHUSDT'
        interval: str = cfg.DEFAULT_INTERVAL,  # '1d'
        ) -> None:
    """Adds missing data of trading pair to local storage"""
    found_dates = get_history_dates(target_pair, interval)
    miss_months, miss_days = filter_missing_data(target_pair, found_dates, interval)
    success_rows, failed_dts = download_missing_data(target_pair, miss_months, miss_days, interval)
    failed_qty = sum([len(i[1]) for i in failed_dts.items()])
    failed_print = '' if failed_qty == 0 else dict(failed_dts)
    print(f'{success_rows} new rows of data obtained, {failed_qty} operations failed {failed_print}')
    return
