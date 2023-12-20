import pandas as pd
import os
from support_funcs import *
from pprint import pprint
import json

columns_to_analyse = []

DATASET_NUM = 5
DATA_PATH = './data/'
FILENAME = '[5]asteroid.zip'
file_size = os.path.getsize(f'{DATA_PATH}{FILENAME}')
RESULT_PATH = f'./result/data{DATASET_NUM}/'

# переменной SHOW можно менять отображение крупных принтов в консоли
SHOW = False
CHUNKSIZE = 100_000
COLUMNS_PER_ITER = 40
MAX_RAM_MB = 8_192
COMPRESSION = 'zip'

def main():
    start_dtypes = analyse_dataset_through_columns(f'{DATA_PATH}{FILENAME}', CHUNKSIZE, columns_per_iter=COLUMNS_PER_ITER, 
                                                   max_ram_mb=MAX_RAM_MB, show=SHOW, 
                                                   filename_to_dump=f'{RESULT_PATH}before_opt_stats.json',
                                                   compr=COMPRESSION)
    optimized_open_params = downcast_through_columns(f'{DATA_PATH}{FILENAME}', columns_per_iter=COLUMNS_PER_ITER, max_ram_mb=MAX_RAM_MB, 
                                                     open_params=start_dtypes, show=SHOW, compr=COMPRESSION)
    dump_json(optimized_open_params, f'{RESULT_PATH}open_opt_params.json')
    analyse_dataset_through_columns(f'{DATA_PATH}{FILENAME}', CHUNKSIZE, columns_per_iter=COLUMNS_PER_ITER, 
                                    max_ram_mb=MAX_RAM_MB, open_params=optimized_open_params, show=SHOW, 
                                    filename_to_dump=f'{RESULT_PATH}after_opt_stats.json', compr=COMPRESSION)

    # ГРАФИКИ СЮДА


if __name__ == '__main__':
    main()

# df = pd.read_csv(f'name', chunksize=chunksize, compression='gzip')