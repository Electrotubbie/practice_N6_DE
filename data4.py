import pandas as pd
import os
from support_funcs import *
from pprint import pprint
import seaborn as sns
import matplotlib.pyplot as plt

sns.set_style("ticks",{'axes.grid' : True})

columns_to_analyse = ['schedule_name', 'experience_name', 'employer_name', 
                      'employer_name', 'salary_from', 'salary_to', 'archived', 
                      'area_name', 'prof_classes_found', 'accept_incomplete_resumes']

DATASET_NUM = 4
DATA_PATH = './data/'
FILENAME = '[4]vacancies.csv.gz'
file_size = os.path.getsize(f'{DATA_PATH}{FILENAME}')
RESULT_PATH = f'./result/data{DATASET_NUM}/'

# переменной SHOW можно менять отображение крупных принтов в консоли
SHOW = False
CHUNKSIZE = 100_000
COLUMNS_PER_ITER = 40
MAX_RAM_MB = 8_192
COMPRESSION = 'gzip'

def plot1(df):
    sns.heatmap(df.isnull(), cbar = False).set_title("Missing values").get_figure().savefig(f'{RESULT_PATH}1plot.png')

def plot2(df):
    df['schedule_name'].value_counts().plot(kind='pie', title='Count of schedule_name').get_figure().savefig(f'{RESULT_PATH}2plot.png')

def plot3(df):
    sns.barplot(df.groupby(['experience_name', 'archived'], as_index=False)['salary_from'].mean(), 
        hue='archived', 
        x='experience_name', 
        y='salary_from').get_figure().savefig(f'{RESULT_PATH}3plot.png')

def plot4(df):
    plt.figure(figsize=(16,20))
    sns.displot(
        data=df,
        y="experience_name",
        hue="accept_incomplete_resumes",
        multiple="fill",
        aspect=2,
    ).savefig(f'{RESULT_PATH}4plot.png')

def plot5(df):
    sns.pairplot(df).savefig(f'{RESULT_PATH}5plot.png')

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

    # 4.1. Визуальный анализ отсутствующих значений
    plot1(read_to_analyse)

    # 4.2. Круговая диаграмма, отображающая количество вакансий в зависимости от условий занятости
    plot2(read_to_analyse)

    # 4.3. Средняя зп в зависимости от опыта работы
    plot3(read_to_analyse)

    # 4.4. Показатель по тому, с каким опытом больше всего принимают незаконченные резюме
    plot4(read_to_analyse)

    # 4.5. Pairplot
    plot5(read_to_analyse)


if __name__ == '__main__':
    main()