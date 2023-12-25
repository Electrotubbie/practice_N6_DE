import pandas as pd
import os
from support_funcs import *
from pprint import pprint
import seaborn as sns
import matplotlib.pyplot as plt

sns.set_style("ticks",{'axes.grid' : True})

columns_to_analyse = ['Case Number', 'Date', 'Block', 'Primary Type', 'Arrest', 'Domestic', 'Beat', 'District', 'Ward', 'FBI Code']

DATASET_NUM = 6
DATA_PATH = './data/'
FILENAME = 'Chicago Total Crimes 2001 - 2023.csv'
file_size = os.path.getsize(f'{DATA_PATH}{FILENAME}')
RESULT_PATH = f'./result/data{DATASET_NUM}/'

# переменной SHOW можно менять отображение крупных принтов в консоли
SHOW = False
CHUNKSIZE = 100_000
COLUMNS_PER_ITER = 40
MAX_RAM_MB = 8_192
COMPRESSION = None

def plot1(df):
    df['Date'].dt.year.value_counts().plot(kind='bar').get_figure().savefig(f'{RESULT_PATH}1plot.png')

def plot2(df):
    df['Year'] = df['Date'].dt.year
    plt.xticks(rotation=90)
    sns.countplot(x='Year', hue='Arrest', data=df).get_figure().savefig(f'{RESULT_PATH}2plot.png')

def plot3(df):
    df.plot.scatter(x='District', y='Ward').get_figure().savefig(f'{RESULT_PATH}3plot.png')

def plot4(df):
    corr = df[['Date', 'Arrest', 'Domestic', 'Beat', 'District', 'Ward']].corr().abs()
    sns.heatmap(corr, annot=True).get_figure().savefig(f'{RESULT_PATH}4plot.png')

def plot5(df):
    df['Arrest'].value_counts().plot(kind='pie', title='Count of arrested').get_figure().savefig(f'{RESULT_PATH}5plot.png')

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
    read_to_analyse['Date'] = [elem[0] for elem in read_to_analyse['Date'].astype(str).str.split(' ')]
    read_to_analyse['Date'] = pd.to_datetime(read_to_analyse['Date'])

    # 6.1. Количество преступлений в год
    # Видно, что количество преступлений сокращается со временем
    plot1(read_to_analyse)

    # 6.2. Корреляция числовых значений таблицы
    plot2(read_to_analyse)

    # 6.3. Совместное слияние признаков
    plot3(read_to_analyse)

    # 6.4. Корреляция числовых данных
    plot4(read_to_analyse)

    # 6.5. Cоотношение арестованых и нет
    plot5(read_to_analyse)


if __name__ == '__main__':
    main()

# df = pd.read_csv(f'name', chunksize=chunksize, compression='gzip')