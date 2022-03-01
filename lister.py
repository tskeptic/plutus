
import csv
import os
import pathlib
import requests

import configs as cfg
import utils



def get_all_symbols(force: bool = False) -> list:
    """Obtains all possible symbols from binance"""
    fname = f'{cfg.PAIRS_DIR}/possible_symbols.csv'
    if os.path.isfile(fname) and (not force):
        all_symbols = [r for r in csv.reader(open(fname))]
    else:
        # getting data
        url = 'https://api.binance.com/api/v3/exchangeInfo'
        res = requests.get(url)
        if res.status_code != 200:
            return
        target_fields = ['symbol', 'baseAsset', 'quoteAsset']
        sym = [x for x in res.json()['symbols']]
        sym = [[x[f] for f in target_fields] for x in sym]
        all_symbols = sorted(sym, key=lambda x: x[0])
        # storing
        _ = utils.write_csv_file(fname, all_symbols)
    return all_symbols


def get_symbols_from_transactions() -> list:
    """Gets list of pairs transacted from processed files"""
    all_transact = [str(p) for p in pathlib.Path(cfg.TRANSACTIONS_DIR).rglob('**/transactions_*')]
    t_fields = ['pair', 'ticker', 'total_ticker']
    transact_pairs = [tuple(i[f] for f in t_fields) for subl in all_transact for i in utils.read_jsonlines_file(subl)]
    transact_pairs = set(transact_pairs)
    transact_pairs = sorted([list(x) for x in transact_pairs], key=lambda k: k[0])
    return transact_pairs


def generate_list_of_interest(
        symb_pop: list,
        trans: list,
        plog: bool = True,
        ) -> list:
    """Generates list of symbols of interest for analysis"""
    # prep
    pairs_pop = [x[0] for x in symb_pop]
    trans_asis = [s[0] for s in trans]
    trans_usd = [f'{s[1]}USDT' for s in trans]
    # masks
    adds_mask = [s in pairs_pop for s in trans_asis]
    adds_usd_mask = [s in pairs_pop for s in trans_usd]
    # logging
    if plog:
        not_found = []
        if not all(adds_mask):
            not_found.extend([i for i, b in zip(trans_asis, adds_mask) if not b])
        if not all(adds_usd_mask):
            not_found.extend([i for i, b in zip(trans_usd, adds_usd_mask) if not b])
        if len(not_found) > 0:
            print(f'pairs not available: {set(not_found)}')
    # final list
    target_symbols = ['USDTBRL']
    target_symbols.extend([i for i, b in zip(trans_asis, adds_mask) if b])
    target_symbols.extend([i for i, b in zip(trans_usd, adds_usd_mask) if b])
    return sorted(list(set(target_symbols)))


def prepare_target_pairs(force: bool = False) -> list:
    all_symbols = get_all_symbols(force)
    transact_symbols = get_symbols_from_transactions()
    my_symbols = generate_list_of_interest(all_symbols, transact_symbols)
    fname = f'{cfg.PAIRS_DIR}/target_symbols.csv'
    n = utils.write_csv_file(fname, my_symbols)
    return my_symbols
