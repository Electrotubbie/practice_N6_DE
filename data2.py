import pandas as pd
import os
from support_funcs import *
from pprint import pprint
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import math

sns.set_style("ticks",{'axes.grid' : True})

columns_to_analyse = ['msrp', 'isNew', 'brandName', 'dealerID', 
                      'vf_ABS', 'vf_FuelTypePrimary', 'vf_Seats', 
                      'vf_TopSpeedMPH', 'vf_TransmissionStyle', 'vf_ModelYear']

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

def plot1(df):
    plt.figure(figsize=(10,5))
    df.groupby(df['vf_ModelYear'])["vf_TopSpeedMPH"].mean().plot().get_figure().savefig(f'{RESULT_PATH}1plot_avg_speed_by_year.png')

def plot2(df):
    sns.heatmap(df.isnull(), cbar = False).set_title("Missing values").get_figure().savefig(f'{RESULT_PATH}2plot_missing_values.png')

def plot3(df):
    g = sns.FacetGrid(df, col='isNew')
    g.map(plt.hist, 'vf_Seats', bins=20).savefig(f'{RESULT_PATH}3plot_seats_by_isNew.png')

def plot4(df):
    sns.scatterplot(data=df, x='vf_ModelYear', y='vf_FuelTypePrimary', hue="isNew").get_figure().savefig(f'{RESULT_PATH}4plot_modelyear_and_fuelType_by_isNew.png')

def plot5(df):
    df['vf_FuelTypePrimary'].value_counts().apply(math.log).plot(kind='bar').get_figure().savefig(f'{RESULT_PATH}5plot_fueltype_counts.png')

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
    
    params_to_read = read_json(f'{RESULT_PATH}open_opt_params.json')
    read_to_analyse = pd.read_csv(f'{DATA_PATH}{FILENAME}', 
                                  usecols=columns_to_analyse,
                                  dtype=params_to_read)
    read_to_analyse.to_csv(f'{DATA_PATH}data_{DATASET_NUM}.csv')
    print(f'Объём RAM прочитанного датасета : {read_to_analyse.memory_usage(deep=True).sum() // (1024**2)} MB')

    # 2.1. График показывает среднюю максимальную скорость автомобилей, произведённых в определённый год
    # предполагал, что максимальная скорость авто должна расти со временем, в целом выглядит всё примерно так, но как-то не явно
    plot1(read_to_analyse)

    # 2.2. Визуальный анализ отсутствующих значений
    plot2(read_to_analyse)

    # 2.3. Сравнение количества новых и не невых авто по отношению к количеству сидений в них
    # заметно, что количество 5местных авто преобладает, 
    # но в целом видно, что все количество для новых и б/у авто примерно одинаково
    plot3(read_to_analyse)

    # 2.4 График отображает использование топливной системы в зависимости от года модели
    # из графика видно, что больше дольше всего на рынке авто производятся машины на ДВС,
    # к 2010 году появились электрокары
    # plot4(read_to_analyse)

    # 2.5 сравенение количества топливных систем среди автомобилей
    plot5(read_to_analyse)


if __name__ == '__main__':
    main()

# df = pd.read_csv(f'name', chunksize=chunksize, compression='gzip')