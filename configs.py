"""
Configuration Variables
"""

# directories
DATA_DIR='data'
PAIRS_DIR=DATA_DIR+'/pairs'
PAIRS_DATA_DIRTEMPLATE=PAIRS_DIR+'/{trade_pair}/{interval}'

# files
PRICES_FILETEMPLATE='{trade_pair}-{yearmonth}-{interval}.jl.gz'
PAIRS_AVAILABILITY_METADATA='availability_metadata.jl'

# ?? (other)
HISTORY_START_DATE='2020-01-01'
SUPPORTED_INTERVALS=['1d']
DEFAULT_INTERVAL='1d'
TQDM_NCOLS=70
