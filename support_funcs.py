import json
import pandas as pd
import os

def dump_json(obj, filename):
    with open(filename, mode='w', encoding='UTF-8') as f:
        json.dump(obj, f, ensure_ascii=False)

def read_json(filename):
    with open(filename, mode='r', encoding='UTF-8') as f:
        return json.load(f)

def dump_dataset_params(optimized_df, usecols, filename):
    params = {
        column_name: optimized_df[column_name].dtype.name
        for column_name in usecols
    }
    dump_json(params, filename)

def memory_analyse(df, file_size, filename = None, show = False):
    memory_usage_stat = df.memory_usage(deep=True)
    total_memory_usage = float(memory_usage_stat.sum())
    columns_stats = sorted([
        {
            'column_name': column,
            'memory_abs': int(memory_usage_stat[column]) // 1024,
            'memory_per': round(memory_usage_stat[column] / total_memory_usage * 100, 4),
            'dtype': str(df.dtypes[column])
        }
        for column in df
    ], key=lambda x: x['memory_abs'], reverse=True)
    to_dump = {
        'file_size': file_size // 1024,
        'file_in_memory_size': total_memory_usage // 1024,
        'columns_stats': columns_stats
    }
    if filename:
        dump_json(to_dump, filename)
    if show:
        print(f'file size           = {file_size // 1024:10} КБ')
        print(f'file in memory size = {total_memory_usage // 1024:10} КБ')
        for col in columns_stats:
            print(f'{col["column_name"]:30}: \
                    {col["memory_abs"]:5} КБ: \
                    {col["memory_per"]:5} %: \
                    {col["dtype"]}')

def categorial_transform(df):
    K = 0.5 # максимальное содержание уникальных записей среди всех
    for column in df.select_dtypes(include=['object']):
        all = len(df[column]) # все записи
        uniq = len(df[column].unique()) # уникальные
        if uniq / all < K:
            # приведение типов object к категориальному типу данных при соблюдении условия 
            df[column] = df[column].astype('category')
    return df

def downcast_int_column(column):
    # проверка на чило без знака
    unsigned = False not in set(column >= 0)
    if unsigned:
        column = pd.to_numeric(column, downcast='unsigned')
    else:
        column = pd.to_numeric(column, downcast='signed')
    return column

def downcast_float_column(column):
    column = pd.to_numeric(column, downcast='float')
    return column

def downcasting_digits(df, show = False):
    before = df.select_dtypes(include=['int', 'float']).dtypes
    for column in df.select_dtypes(include=['int', 'float']):
        if 'int' in df[column].dtype.name:
            df[column] = downcast_int_column(df[column]) 
        elif 'float' in df[column].dtype.name:
            df[column] = downcast_float_column(df[column])
    after = df.select_dtypes(include=['int', 'float']).dtypes
    compare = pd.DataFrame({'before': before, 'after': after})
    if show:
        print(compare)
    return df

def analyse_dataset_through_columns(filename_to_analyse, 
                                    chunksize, 
                                    columns_per_iter = 5, 
                                    max_ram_mb = 4096, 
                                    open_params = None, 
                                    filename_to_dump = None, 
                                    show = False, 
                                    compr = None,
                                    delimiter = None):
    file_size = os.path.getsize(filename_to_analyse)
    # подготовка данных
    start_data = pd.read_csv(filename_to_analyse, nrows=5, compression=compr, delimiter=delimiter).columns
    columns_stats = {
        column: {}
        for column in start_data
    }
    # анализ датасета по колонкам и суммирование RAM по колонкам отдельно
    total_columns = 0
    volume_rows_cols = 0
    volume_columns_cols = 0
    for i in range(0, len(columns_stats.keys()), columns_per_iter):
        column_df = pd.read_csv(filename_to_analyse, 
                                usecols=list(columns_stats.keys())[i:i+columns_per_iter], 
                                dtype=open_params, 
                                compression=compr,
                                delimiter=delimiter)
        memory = column_df.memory_usage(deep=True)
        if (memory.sum() // (1024**2)) > max_ram_mb:
            print(f'Объём памяти дипазона колонок превышен: {memory.sum()}. \
                   Увеличьте max_ram_mb={max_ram_mb} или уменьшите columns_per_iter={columns_per_iter}.')
            return None
        row_count = len(column_df.index)
        print(f'Количество строк: {row_count}. Объём памяти RAM для части колонок: {memory.sum() // 1024} KB')
        total_columns += memory.sum()
        volume_rows_cols = row_count
        volume_columns_cols += len(column_df.columns)
        for column in column_df:
            columns_stats[column]['dtype'] = str(column_df.dtypes[column])
            columns_stats[column]['memory_abs'] = float(memory[column])
            if show:
                print(f'{column:30}: {columns_stats[column]["dtype"]:15}: {columns_stats[column]["memory_abs"] // 1024:15} KB')
    # расчёт суммарной RAM по чанкам с учётом исследованых dtypes
    total_memory_usage = 0
    if not open_params:
        open_params = {
            column: columns_stats[column]['dtype']
            for column in columns_stats
        }
    volume_columns_chunks = 0
    volume_rows_cols_chunks = 0
    for chunk in pd.read_csv(filename_to_analyse, 
                             chunksize=chunksize, 
                             dtype=open_params, 
                             compression=compr,
                             delimiter=delimiter):
        chunk_memory_usage_stat = chunk.memory_usage(deep=True)
        total_memory_usage += float(chunk_memory_usage_stat.sum()) # B
        volume_rows_cols_chunks += len(chunk.index)
        volume_columns_chunks = len(chunk.columns)
    # расчёт относительного количества памяти для столбцов и приведение к KB
    for col in columns_stats.keys():
        columns_stats[col]['memory_per'] = round(columns_stats[col]['memory_abs'] / total_memory_usage * 100, 4) # B/B * 100 = %
        columns_stats[col]['memory_abs'] = columns_stats[col]['memory_abs'] // 1024 # KB
    # подготовка словаря к дампу
    columns_stats = dict(sorted(list(columns_stats.items()), key=lambda x: x[1]['memory_abs'], reverse=True))
    to_dump = {
        'file_size': file_size // 1024, # KB
        'file_in_memory_size': int(total_memory_usage // 1024), # KB
        'columns_stats': columns_stats
    }
    print(f'Суммарный объём RAM, занимаемый файлом через колонки:    {total_columns // 1024}, объём полей таблицы по колонкам: r{volume_rows_cols} c{volume_columns_cols}')
    print(f'Суммарный объём RAM, занимаемый файлом через чанки:      {to_dump["file_in_memory_size"]}, объём полей таблицы по чанкам:    r{volume_rows_cols_chunks} c{volume_columns_chunks}')
    if filename_to_dump:
        dump_json(to_dump, filename_to_dump)
    # отображение информации
    if show:
        print(f'file size           = {file_size // 1024:10} КБ')
        print(f'file in memory size = {total_memory_usage // 1024:10} КБ')
        for col in columns_stats.keys():
            print(f'{col:30}: \
                    {columns_stats[col]["memory_abs"]:5} КБ: \
                    {columns_stats[col]["memory_per"]:5} %: \
                    {columns_stats[col]["dtype"]}')
    
    return open_params

def downcast_through_columns(filename_to_analyse, 
                             columns_per_iter = 5, 
                             max_ram_mb = 4096, 
                             open_params = None, 
                             show = False, 
                             compr = None,
                             delimiter=None):
    columns = pd.read_csv(filename_to_analyse, nrows=5, compression=compr, delimiter=delimiter).columns
    # анализ датасета по колонкам и суммирование RAM по колонкам отдельно
    optimized_open_params = dict()
    for i in range(0, len(columns), columns_per_iter):
        column_df = pd.read_csv(filename_to_analyse, 
                                usecols=columns[i:i+columns_per_iter], 
                                dtype=open_params, 
                                compression=compr, 
                                delimiter=delimiter)
        memory = column_df.memory_usage(deep=True)
        if (memory.sum() // (1024**2)) > max_ram_mb:
            print(f'Объём памяти дипазона колонок превышен: {memory.sum()}. \
                   Увеличьте max_ram_mb={max_ram_mb} или уменьшите columns_per_iter={columns_per_iter}.')
            return None
        before = column_df.dtypes
        column_df = categorial_transform(column_df)
        column_df = downcasting_digits(column_df)
        after = column_df.dtypes
        compare = pd.DataFrame({'before': before, 'after': after})
        if show:
            print(compare)
        for column in column_df:
            optimized_open_params[column] = column_df[column].dtype.name

    return optimized_open_params