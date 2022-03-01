
import collections
import csv
import datetime
import math
import os
import pathlib
import pytz

import configs as cfg
import utils


## BTCTRD ###########################################################


def parse_sp_dttm(dttm: str, fmt: str = '%d/%m/%Y %H:%M') -> str:
    """Parses string datetime from Brazil to UTC"""
    dttm_obj = datetime.datetime.strptime(dttm, fmt)
    local_dt = pytz.timezone('America/Sao_Paulo').localize(dttm_obj)
    utc_dt = local_dt.astimezone(pytz.utc)
    return str(utc_dt)[:19]


def mult_replaces(val: str, rep_tups: list) -> float:
    """Applies replaces using provided tuples of (from, to)"""
    for r_tuple in rep_tups:
        val = val.replace(*r_tuple)
    return val


def parse_btctrd_value(val: str) -> float:
    """Parses value field from btctrd data source"""
    rep_list=[('BTC ', ''), ('R$ ', ''), ('.', ''), (',', '.')]
    val = mult_replaces(val, rep_list)
    return float(val)


def parse_btctrd_data(rows_list: list) -> list:
    """Parses data from btctrd csv file"""
    # checking expected format
    expected_cols = ['Data', 'Moeda', 'Categoria', 'Valor', 'Saldo após']
    found_cols = rows_list[0]
    text = f'pls check format, expected: {expected_cols}, found: {found_cols}'
    assert found_cols == expected_cols, text
    # checking used coins
    expected_coins = ['Bitcoin', 'Real']
    checks = [x[1] in expected_coins for x in rows_list[1:]]
    assert all(checks), 'unexpected coin'
    # checking matching coins rows only
    coins = [mult_replaces(x[1], [('Bitcoin', 'BTC'), ('Real', 'R$')]) for x in rows_list[1:]]
    coins_check = all([v.count(c)>0 for c,v in zip(coins, [x[3] for x in rows_list[1:]])])
    assert coins_check, 'unmatched coin or coin value'
    # transforming and parsing
    coin_map = {'Bitcoin': 'BTC', 'Real': 'BRL'}
    act_map = {
        'Compra': 'buy',
        'Depósito bancário': 'deposit',
        'Retirada para carteira externa': 'withdraw',
        'Taxa de mineração, baixa prioridade': 'withdrawal_fee',
        'Taxa sobre compra - Executada': 'buy_fee',
        'Taxa sobre compra - Executora': 'buy_fee',
        }
    parsers = {
        'dttm': (0, parse_sp_dttm),
        'coin': (1, coin_map.get),
        'act': (2, act_map.get),
        'value': (3, parse_btctrd_value),
        'balance': (4, parse_btctrd_value),
        }
    new_data = []
    for row in rows_list[1:]:
        new_row = {}
        for col in parsers.keys():
            new_row[col] = parsers[col][1](row[parsers[col][0]])
        new_data.append(new_row)
    return new_data


def prepare_transaction(t: dict) -> dict:
    """Transforms btctrd transaction to default format"""
    return {
        'source': 'btctrd',
        'datetime': t['dttm'],
        'pair': 'BTCBRL',
        'ticker': 'BTC',
        'qty': t['BTC'],
        'total': t['BRL'],
        'total_ticker': 'BRL',
        'fee': t['buy_fee'],
        'fee_ticker': 'BTC',
        }


def get_transactions_from_btctrd(data_rows: list) -> list:
    """Processes operations and transforms into unified transactions"""
    # ordering
    data_rows = sorted(data_rows, key=lambda x: (x['dttm'], x['act'], x['coin']))
    # variables
    transactions = []
    current_transaction = collections.defaultdict(float)
    # iterating
    for ind in range(len(data_rows)):
        # defining current row and next dttm
        row = data_rows[ind]
        next_dttm = data_rows[ind+1]['dttm'] if ind+1 < len(data_rows) else None
        # checking type of action
        if not row['act'].startswith('buy'):
            continue
        # updating transaction
        if row['act'] == 'buy':
            current_transaction[row['coin']] += abs(row['value'])
        if row['act'] == 'buy_fee':
            current_transaction['buy_fee'] += abs(row['value'])
        # checking if transaction is finished
        last_dttm = row['dttm'] != next_dttm
        fee_perc = current_transaction['buy_fee'] / (current_transaction['BTC']+1e-21)
        fee_reasonable = any([math.isclose(fee_perc, f, abs_tol=5e-5) for f in [0.0025, 0.005]])
        if last_dttm and fee_reasonable:
            current_transaction['dttm'] = row['dttm']
            transactions.append(dict(current_transaction))
            current_transaction = collections.defaultdict(float)
    # transforming
    final_transacs = [prepare_transaction(t) for t in transactions]
    return final_transacs


def process_btctrd_data():
    """Prepares data from source btctrd to transactions formatted as expected"""
    # finding file
    btctrade_files = [str(p) for p in pathlib.Path(cfg.MANUAL_TRANSACTIONS_DIR).rglob('**/btctrd_extrato*')]
    assert len(btctrade_files) < 2, 'more than one file found for this source of transactions'
    assert len(btctrade_files) != 0, 'no file found for this source of transactions, make sure the name starts with "btctrd_extrato"'
    # processing
    btctrd_rows_raw = [r for r in csv.reader(open(btctrade_files[0]))]
    btctrd_rows = parse_btctrd_data(btctrd_rows_raw)
    btctrd_transactions = get_transactions_from_btctrd(btctrd_rows)
    # storing results
    f_out = f'{cfg.TRANSACTIONS_DIR}/transactions_btctrd.jl'
    if os.path.isfile(f_out):
        os.remove(f_out)
    n = utils.write_jsonlines_file(f_out, btctrd_transactions)
    print(f'transactions data for btctrd completed, {n} transactions found')
