from datetime import datetime as dt
import pandas as pd
from work_sqlite import *
import os



def create_schedule(path_timetable):
    timetable = pd.read_excel(path_timetable)
    timetable = timetable.iloc[19:38]
    schedule ={}
    month = 12
    year = 2022
    for i in range(len(timetable)):
        series = timetable.iloc[i]
        name = series[1]
        schedule[name] = []
        only_days = series['Unnamed: 3':'Unnamed: 33']
        for count, elem in enumerate(only_days, 1):
            if isinstance(elem, float | int) and elem > 0:
                schedule[name].append(dt.strptime(f'{count}.{month}.{year}', '%d.%m.%Y'))
    return schedule


def create_losses(path_losses_table):
    losses = pd.read_excel(path_losses_table)
    losses = losses.loc[1:71788]
    for i in range(len(losses)):
        losses['Unnamed: 1'].iloc[i] = dt.strptime(losses['Unnamed: 1'].iloc[i],'%d/%m/%y')
    list_losses = [(losses['Unnamed: 1'].iloc[i], float(losses['Unnamed: 6'].iloc[i])) for i in range(len(losses))]
    return list_losses


def save_report(labels, values, name):
    result = {}
    for elem in values:
        for i in range(len(labels)):
            result[labels[i]] = result.get(labels[i],[]) + [elem[i]]
    result_to_excel = pd.DataFrame(result)
    result_to_excel.to_excel(f'{name}.xlsx')
    

schedule = create_schedule('Книга2.xlsx')
list_losses = create_losses('Книга1.xlsx')

connect = create_connection('db_for_sb.db')
[execute_query(connect, query) for query in (query_create_emp, query_create_jd, query_create_loss)]
insert_employees(connect, query_insert_emp, schedule)
insert_Job_Days(connect, query_insert_jd, schedule)
insert_losses(connect, query_insert_loss, list_losses)

for query, name in ((query_select_result, 'report'), (query_select_result_distinct, 'report_distinct')):
    column_names, values = execute_read_query(connect, query)
    save_report(column_names, values, name)

[execute_query(connect, query) for query in (query_drop_emp, query_drop_jd, query_drop_loss)]
connect.close()

print('Отчет успешно сохранен')




