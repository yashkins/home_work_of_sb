from datetime import datetime as dt
import locale
import pandas as pd
import re
from work_sqlite import *
import os


def search_start_stop(table, patern):
    start, stop = None, None
    for i, elem in enumerate(table['Unnamed: 1']):
        if isinstance(elem, str) and re.search(patern, elem):
            start = i
            break
    for i, elem in enumerate(table['Unnamed: 1'][::-1]):
        if isinstance(elem, str) and re.search(patern, elem):
            stop = len(table)-i
            break
    return start, stop


def check_year():
    flag = True
    while flag:
        year = input("Введите год в формате: 'гггг' ")
        if not re.search('202\d', year):
            print('Неверный формат года')
        else:
            flag = False
    return year


def check_data(schedule, list_losses, year, month):
    print(month)
    list_text_error = [f'Отчет сформирован до {month} месяца', 
    'Недостаточно данных для составления отчета', 
    'Введенный год не совпадает с годом в таблице недостач']
    list_bool = [schedule == 'end', not schedule or not list_losses, list_losses and list_losses[0][0].strftime('%Y') != year]
    for i in range(len(list_text_error)):
        if list_bool[i]:
            return list_text_error[i]
    return 


def create_schedule(path_timetable, year, month, day_month):
    try:
        timetable = pd.read_excel(path_timetable, sheet_name=month)
    except ValueError:
        return 'end'
    if len(timetable) == 0:
        return
    search_patern = r'[А-Яа-я]+\s[А-яа-я]+?\.'
    start, stop = search_start_stop(timetable, search_patern)
    if not start:
        return
    timetable = timetable.iloc[start:stop]
    schedule ={}
    name_columns = timetable.T.index
    start_days = name_columns[3]
    stop_days = name_columns[2+day_month]
    for i in range(len(timetable)):
        if timetable.iloc[i].any():
            series = timetable.iloc[i]
            name = series[1]
            schedule[name] = []
            only_days = series[start_days:stop_days]
            for count, elem in enumerate(only_days, 1):
                if isinstance(elem, float | int) and elem > 0:
                    schedule[name].append(dt.strptime(f'{count}.{month}.{year}', '%d.%B.%Y'))
    return schedule


def create_losses(path_losses_table):
    losses = pd.read_excel(path_losses_table)
    if len(losses) == 0:
        return
    search_patern = r'\d+/\d+/\d+'
    start, stop = search_start_stop(losses, search_patern)
    if not start:
        return
    losses = losses.loc[start:stop]
    for i in range(len(losses)):
        if losses.iloc[i].any():
            losses['Unnamed: 1'].iloc[i] = dt.strptime(losses['Unnamed: 1'].iloc[i],'%d/%m/%y')
    list_losses = [(losses['Unnamed: 1'].iloc[i], float(losses['Unnamed: 6'].iloc[i])) for i in range(len(losses)) if losses.iloc[i].any()]
    return list_losses


def save_report(labels, values, name):
    folder_for_reports = os.path.join(os.getcwd(), 'Reports')
    if not os.path.exists(os.path.join(os.getcwd(), 'Reports')):
        os.mkdir(os.path.join(os.getcwd(), 'Reports'))
    result = {}
    for elem in values:
        for i in range(len(labels)):
            result[labels[i]] = result.get(labels[i],[]) + [elem[i]]
    result_to_excel = pd.DataFrame(result)
    result_to_excel.to_excel(os.path.join(folder_for_reports, f'{name}.xlsx'))
    

def create_report(path_timetable, path_losses_table):
    locale.setlocale(category=locale.LC_ALL,locale="Russian")
    year = check_year()
    print(year)
    dict_month = {"январь":31, "февраль":28 if int(year) % 4 else 29, "март":31, "апрель":30, 
                "май":31, "июнь":30, "июль":31, "август":31, "сентябрь":30, 
                "октябрь":31, "ноябрь":30, "декабрь":31} 

    for month in dict_month:
        schedule = create_schedule(path_timetable, year, month, dict_month[month])
        if month == 'январь':
            list_losses = create_losses(path_losses_table)

        error = check_data(schedule, list_losses, year, month)
        if error:
            print(error) 
            return   
        
        connect = create_connection('db_for_sb.db')
        [execute_query(connect, query) for query in (query_create_emp, query_create_jd, query_create_loss)]
        insert_employees(connect, query_insert_emp, schedule)
        insert_Job_Days(connect, query_insert_jd, schedule)
        insert_losses(connect, query_insert_loss, list_losses)

        for query, name in ((query_select_result, month), (query_select_result_distinct, f'{month}_distinct')):
            column_names, values = execute_read_query(connect, query)
            save_report(column_names, values, name)

        execute_query(connect, query_drop_emp) 
        execute_query(connect, query_drop_jd)
        execute_query(connect, query_drop_loss)
        connect.close()

        print('Отчет успешно сохранен')

if __name__ == "__main__":
    create_report('Книга2.xlsx', 'Книга1.xlsx')




