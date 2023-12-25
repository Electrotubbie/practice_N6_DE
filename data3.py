import pandas as pd
import os
from support_funcs import *
from pprint import pprint
import seaborn as sns
import matplotlib.pyplot as plt

sns.set_style("ticks",{'axes.grid' : True})

columns_to_analyse = ['YEAR', 'DAY_OF_WEEK', 'AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'DEPARTURE_DELAY', 'DISTANCE', 'ELAPSED_TIME', 'ARRIVAL_DELAY', 'CANCELLED']

DATASET_NUM = 3
DATA_PATH = './data/'
FILENAME = '[3]flights.csv'
file_size = os.path.getsize(f'{DATA_PATH}{FILENAME}')
RESULT_PATH = f'./result/data{DATASET_NUM}/'

# переменной SHOW можно менять отображение крупных принтов в консоли
SHOW = False
CHUNKSIZE = 100_000
COLUMNS_PER_ITER = 40
MAX_RAM_MB = 8_192
COMPRESSION = None
DELIMITER = ';'

def plot1(df):
    sns.heatmap(df.isnull(), cbar = False).set_title("Missing values").get_figure().savefig(f'{RESULT_PATH}1plot_missing_values.png')

def plot2(df):
    airport_list = ['ANC', 'LAX', 'SFO', 'SEA', 'LAS', 'CDV', 'LWS', 'ABQ', 'ABR']
    mean_delay_by_airport = df.groupby('ORIGIN_AIRPORT')['DEPARTURE_DELAY'].mean().sort_values(ascending=False)
    mean_delay_by_airport[mean_delay_by_airport.index.isin(airport_list)].plot(kind='bar', title='DEPARTURE DELAY BY ORIGIN AIRPORT').get_figure().savefig(f'{RESULT_PATH}2plot_mean_dep_delay.png')

def plot3(df):
    df['AIRLINE'].value_counts().plot(kind='pie', title='Count of airlines').get_figure().savefig(f'{RESULT_PATH}3plot_Count of airlines.png')

def plot4(df):
    plt.figure(figsize=(16,20))
    sns.displot(
        data=df,
        y="AIRLINE",
        hue="CANCELLED",
        multiple="fill",
        aspect=2,
    ).savefig(f'{RESULT_PATH}4plot_CANCELLED.png')

def plot5(df):
    sns.pairplot(df).savefig(f'{RESULT_PATH}5plot_pairplot.png')


def main():
    start_dtypes = analyse_dataset_through_columns(f'{DATA_PATH}{FILENAME}', CHUNKSIZE, columns_per_iter=COLUMNS_PER_ITER, 
                                                   max_ram_mb=MAX_RAM_MB, show=SHOW, 
                                                   filename_to_dump=f'{RESULT_PATH}before_opt_stats.json',
                                                   compr=COMPRESSION, delimiter=DELIMITER)
    optimized_open_params = downcast_through_columns(f'{DATA_PATH}{FILENAME}', columns_per_iter=COLUMNS_PER_ITER, max_ram_mb=MAX_RAM_MB, 
                                                     open_params=start_dtypes, show=SHOW, compr=COMPRESSION, delimiter=DELIMITER)
    dump_json(optimized_open_params, f'{RESULT_PATH}open_opt_params.json')
    analyse_dataset_through_columns(f'{DATA_PATH}{FILENAME}', CHUNKSIZE, columns_per_iter=COLUMNS_PER_ITER, 
                                    max_ram_mb=MAX_RAM_MB, open_params=optimized_open_params, show=SHOW, 
                                    filename_to_dump=f'{RESULT_PATH}after_opt_stats.json', compr=COMPRESSION,
                                    delimiter=DELIMITER)
    
    params_to_read = read_json(f'{RESULT_PATH}open_opt_params.json')
    read_to_analyse = pd.read_csv(f'{DATA_PATH}{FILENAME}', 
                                  usecols=columns_to_analyse,
                                  dtype=params_to_read, 
                                  delimiter=';')
    read_to_analyse.to_csv(f'{DATA_PATH}data_{DATASET_NUM}.csv')
    print(f'Объём RAM прочитанного датасета : {read_to_analyse.memory_usage(deep=True).sum() // (1024**2)} MB')

    # 3.1. Визуальный анализ отсутствующих значений
    plot1(read_to_analyse)

    # 3.2. Средняя задержка по аэропорту отправления согласно списку
    plot2(read_to_analyse)

    # 3.3. Круговая диаграмма, отображающая количество авиакомпаний по всему датасету
    plot3(read_to_analyse)

    # 3.4. Отображение отменённых рейсов по авиакомпаниям
    plot4(read_to_analyse)

    # 3.5. Pairplot
    plot5(read_to_analyse)


if __name__ == '__main__':
    main()

# df = pd.read_csv(f'name', chunksize=chunksize, compression='gzip')