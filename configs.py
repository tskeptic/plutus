"""
Configuration Variables
"""

# directories
DATA_DIR='data'
PAIRS_DATA_DIRTEMPLATE=DATA_DIR+'/pairs/{trade_pair}/{interval}'

# files
PRICES_FILETEMPLATE='{trade_pair}-{yearmonth}-{interval}.jl.gz'

# ?? (other)
HISTORY_START_DATE='2020-01-01'
SUPPORTED_INTERVALS=['1d']
DEFAULT_INTERVAL='1d'
TQDM_NCOLS=70
