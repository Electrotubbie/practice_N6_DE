import pandas as pd
import os
from support_funcs import *
from pprint import pprint
import json

columns_to_analyse = []

DATASET_NUM = 2
DATA_PATH = './data/'
FILENAME = '[2]automotive.csv.zip'
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
        

    # чтение исходного датасета согласно ранее сохранённым параметрам
    # params_to_read = read_json(f'{RESULT_PATH}open_opt_params.json')
    # read_to_analyse = pd.read_csv(f'{DATA_PATH}{FILENAME}', 
    #                               usecols=params_to_read.keys(),
    #                               dtype=params_to_read,
    #                               parse_dates=['date'],
    #                               compression='zip')

    # ГРАФИКИ СЮДА


if __name__ == '__main__':
    main()

# df = pd.read_csv(f'name', chunksize=chunksize, compression='gzip')