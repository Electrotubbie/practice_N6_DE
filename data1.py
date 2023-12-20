import pandas as pd
import os
from support_funcs import *
from pprint import pprint
import json
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import json
sns.set_style("ticks",{'axes.grid' : True})

columns_to_analyse = ['date', 'day_of_week', 'v_score', 'h_score', 
                      'day_night', 'length_minutes', 'v_assists', 
                      'v_errors', 'h_assists', 'h_errors']

# переменной SHOW можно менять отображение крупных принтов в консоли
SHOW = False
DATASET_NUM = 1
DATA_PATH = './data/'
FILENAME = '[1]game_logs.csv'
file_size = os.path.getsize(f'{DATA_PATH}{FILENAME}')
RESULT_PATH = f'./result/data{DATASET_NUM}/'

def optimize(df):
    df = categorial_transform(df)
    df = downcasting_digits(df, show=SHOW)

    return df

def main():
    df = pd.read_csv(f'{DATA_PATH}{FILENAME}')
    # анализ исходного датасета
    memory_analyse(df, file_size=file_size, filename=f'{RESULT_PATH}before_opt_stats.json', show=SHOW)
    print(f'Объём RAM до преобразования     : {df.memory_usage(deep=True).sum() // (1024**2)} MB')
    # оптимизация исходного датасета
    optimized_df = optimize(df)
    # анализ оптимизацированного датасета
    memory_analyse(optimized_df, file_size=file_size, filename=f'{RESULT_PATH}after_opt_stats.json', show=SHOW)
    print(f'Объём RAM после преобразования  : {df.memory_usage(deep=True).sum() // (1024**2)} MB')
    # сохранение параметров открытия датасета для последующего анализа
    dump_dataset_params(df, columns_to_analyse, f'{RESULT_PATH}open_opt_params.json')
    # чтение исходного датасета согласно ранее сохранённым параметрам
    params_to_read = read_json(f'{RESULT_PATH}open_opt_params.json')
    read_to_analyse = pd.read_csv(f'{DATA_PATH}{FILENAME}', 
                                  usecols=params_to_read.keys(),
                                  dtype=params_to_read,
                                  parse_dates=['date'])
    read_to_analyse.to_csv(f'{DATA_PATH}data_{DATASET_NUM}.csv')
    print(f'Объём RAM прочитанного датасета : {read_to_analyse.memory_usage(deep=True).sum() // (1024**2)} MB')


    # 1. График показывает среднюю продолжительность матча за год
    # из графика видно, что ранее матчи были короче 
    plt.figure(figsize=(10,5))
    plot = read_to_analyse.groupby(read_to_analyse['date'].dt.year)["length_minutes"].mean().plot()
    plot.get_figure().savefig(f'{RESULT_PATH}1plot_avg_length_per_year.png')

    # 2. график для сравнения набранных очков за матч при игре дома (оранжевый) и в гостях (синий)
    # можно отметить, что дома количество очков выше, чем в гостях
    # также можно отметить, что до ~1900 годов количество очков было выше, чем сейчас
    plt.figure(figsize=(20,5))
    gr_obj_1 = read_to_analyse.groupby(read_to_analyse['date'].dt.year)["v_score"].mean()
    gr_obj_2 = read_to_analyse.groupby(read_to_analyse['date'].dt.year)["h_score"].mean()
    X_1 = gr_obj_1.index
    Y_1 = gr_obj_1.values
    X_2 = gr_obj_2.index
    Y_2 = gr_obj_2.values
    plt.plot(X_1, Y_1, label="Visit") # синий
    plt.plot(X_2, Y_2, label="Home") # оранжевый
    plt.title('Score')
    plt.xlabel('Year')
    plt.ylabel('Score')
    plt.legend()
    plt.savefig(f'{RESULT_PATH}2plot_compare_scores_hv.png')
    

    # 3. Столбчатая диаграмма, которая отображает количество матчей по дням недели
    # стоит отметить, что больше всего игр было в выходной в Субботу, а меньше всего - в Понедельник
    plt.figure(figsize=(20,5))
    plot = read_to_analyse['day_of_week'].value_counts()[['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']].plot(kind='bar', title='Count of games in day of weeks', x='Day of week', y='Count of games')
    plot.get_figure().savefig(f'{RESULT_PATH}3plot_Count of games in day of weeks.png')

    # 4. Круговая диаграмма количества дневных и ночных матчей.
    plt.figure(figsize=(5,5))
    plot = read_to_analyse['day_night'].value_counts().plot(kind='pie', title='Count of day/night matches')
    plot.get_figure().savefig(f'{RESULT_PATH}4plot_Count of day night matches.png')

    # 5. pairplot
    sns.pairplot(read_to_analyse).savefig(f'{RESULT_PATH}5plot_paiplot.png')


if __name__ == '__main__':
    main()

# df = pd.read_csv(f'name', chunksize=chunksize, compression='gzip')