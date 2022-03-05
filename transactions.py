
import collections
import csv
import datetime
import math
import os
import pathlib
import pytz
import re

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


def parse_btctrd_act_category(val: str) -> str:
    """Parses action category field from btctrd data source"""
    re_groups = [
        ('^Compra$', 'buy'),
        ('^Depósito', 'deposit'),
        ('^Retirada para', 'withdraw'),
        ('^Taxa de mineração', 'withdrawal_fee'),
        ('^Taxa sobre compra', 'buying_fee'),
        ('^Venda', 'sell'),
    ]
    res = []
    for regex in re_groups:
        if re.match(regex[0], val):
            res.append(regex[1])
    assert len(res) == 1, f'unexpected matching action for {val}, groups found: {res}'
    return res[0]


def parse_btctrd_values(row_doc: str) -> float:
    """Parses values fields from btctrd data source"""
    values_cols = ['value', 'balance']
    rep_list = [(row_doc['coin']+' ', ''), ('.', ''), (',', '.')]
    for field in values_cols:
        row_doc[field] = float(mult_replaces(row_doc[field], rep_list))
    return row_doc


def parse_btctrd_data(rows_list: list) -> list:
    """Parses data from btctrd csv file"""
    # checking expected format
    expected_cols = ['Data', 'Moeda', 'Categoria', 'Valor', 'Saldo após']
    expected_cols_check = rows_list[0] == expected_cols
    except_text = f'pls check format, expected: {expected_cols}, found: {rows_list[0]}'
    assert expected_cols_check, except_text
    # transforming and parsing
    coin_map = {'Bitcoin': 'BTC', 'Real': 'R$'}
    parsers = {
        'dttm': (0, parse_sp_dttm),
        'coin': (1, lambda x: coin_map.get(x, x.upper())),
        'act': (2, parse_btctrd_act_category),
        'value': (3, lambda x: x),
        'balance': (4, lambda x: x),
        }
    new_data = []
    for row in rows_list[1:]:
        new_row = {}
        for col in parsers.keys():
            new_row[col] = parsers[col][1](row[parsers[col][0]])
        new_data.append(new_row)
    # checking matching coin and values
    coins_values_match_check = all([r['value'].count(r['coin']) == 1 for r in new_data])
    except_text = 'unmatching coin and value in data'
    assert coins_values_match_check, except_text
    # parsing values
    for row in new_data:
        row = parse_btctrd_values(row)
    return new_data


def prepare_transaction_doc(t: dict) -> dict:
    """Transforms btctrd transaction to default format"""
    return {
        'source': 'btctrd',
        'datetime': t['dttm'],
        'pair': f"{t['coin']}BRL",
        'ticker': t['coin'],
        'qty': t['value'],
        'total': t['paid_value'],
        'total_ticker': 'BRL',
        'fee': t.get('buying_fee', .0),
        'fee_ticker': t['coin'],
        'mov_fee': t['withd'],
        'mov_ticker': t['coin'],
        }


def get_transactions_from_btctrd(data_rows: list, val_tol: float = 9e-08) -> list:
    """Processes operations and transforms into unified transactions"""
    # ordering
    data_rows = sorted(data_rows, key=lambda x: (x['dttm'], x['act'], x['coin']))
    # variables
    all_trans = []
    curr_trans = collections.defaultdict(float)
    # iterating
    for ind in range(len(data_rows)):
        # defining current row and next dttm
        row = data_rows[ind]
        next_row = data_rows[ind+1] if ind+1 < len(data_rows) else {}
        # checking type of action
        if not row['act'].startswith('buy'):
            # TODO: implement sell
            continue
        # checking coin
        row_fiat = row['coin'] == 'R$'
        if not row_fiat:
            if curr_trans['coin'] == .0:
                curr_trans['coin'] = row['coin']
            else:
                same_coin_check = curr_trans['coin'] == row['coin']
                except_text = ("different coins in the same transaction: "
                               f"{curr_trans['coin']} and {row['coin']}")
                assert same_coin_check, except_text
        # getting next withdrawal fee
        if curr_trans['coin'] != 0:
            next_w = [r
                      for r in data_rows[ind:]
                      if (r['act'].startswith('withdraw')) and (r['coin'] == row['coin'])
                      ]
            if len(next_w) > 0:
                withd = next_w[0]
                if len(next_w) > 1:
                    withd_fee = next_w[1]
                else:
                    withd_fee = {}
                curr_trans['next_withdrawal_fee'] = abs(withd_fee.get('value', 0))
                curr_trans['next_withdrawal'] = abs(withd.get('value', 0))
        # updating transaction
        if row['act'] == 'buy':
            if row_fiat:
                curr_trans['paid_value'] += abs(row['value'])
                if abs(row['value']) > val_tol:
                    curr_trans['n_fiat'] += 1
            else:
                curr_trans['value'] += abs(row['value'])
                if abs(row['value']) > val_tol:
                    curr_trans['n_coin'] += 1
        if row['act'] == 'buying_fee':
            assert not row_fiat, 'unexpected buying fee paid in fiat'
            curr_trans['buying_fee'] += abs(row['value'])
        # checking if transaction is finished
        same_dttm = row['dttm'] != next_row.get('dttm')
        at_least_one_op = curr_trans['n_coin'] > 0
        match_ops = curr_trans['n_coin'] == curr_trans['n_fiat']
        if all([same_dttm, at_least_one_op, match_ops]):
            curr_trans['dttm'] = row['dttm']
            # calculating withdrawal cost for this transaction
            if curr_trans['next_withdrawal_fee'] != 0:
                withd_mult = curr_trans['next_withdrawal_fee'] / curr_trans['next_withdrawal']
                curr_trans['withd'] =  curr_trans['value'] * withd_mult
            else:
                curr_trans['withd'] = .0
            all_trans.append(dict(curr_trans))
            curr_trans = collections.defaultdict(float)
    # transforming
    final_transacs = [prepare_transaction_doc(t) for t in all_trans]
    # ordering dict keys
    final_transacs = [dict(sorted(di.items())) for di in final_transacs]
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
    n = utils.write_jsonlines_file(f_out, btctrd_transactions)
    print(f'transactions data for btctrd completed, {n} transactions found')


## OTHER ############################################################


def parse_others_data(rows_list: list) -> list:
    """Parses data from other sources csv file"""
    # checking expected format
    exp_cols = ['source','datetime','ticker','qty','total','total_ticker','fee','fee_ticker']
    found_cols = rows_list[0]
    text = f'pls check format, expected: {exp_cols}, found: {found_cols}'
    assert found_cols == exp_cols, text
    # transforming
    new_data = [{k:v for k,v in zip(exp_cols, r)} for r in rows_list[1:]]
    # changing types
    float_cols = ['qty', 'total', 'fee']
    for r in new_data:
        r['pair'] = ''.join([r['ticker'], r['total_ticker']])
        for c in r.keys():
            if c in float_cols:
                r[c] = float(r[c])
    # ordering dict keys
    new_data = [dict(sorted(di.items())) for di in new_data]
    return new_data


def process_other_sources_data():
    """Prepares data from other source to transactions formatted as expected"""
    # finding file
    other_sources = [str(p) for p in pathlib.Path(cfg.MANUAL_TRANSACTIONS_DIR).rglob('**/other_*')]
    assert len(other_sources) < 2, 'more than one file found for this source of transactions'
    assert len(other_sources) != 0, 'no file found for this source of transactions, make sure the name starts with "other_"'
    # processing
    osources_rows_raw = [r for r in csv.reader(open(other_sources[0]))]
    osources_data = parse_others_data(osources_rows_raw)
    # storing results
    f_out = f'{cfg.TRANSACTIONS_DIR}/transactions_other.jl'
    n = utils.write_jsonlines_file(f_out, osources_data)
    print(f'transactions data for other sources completed, {n} transactions found')
