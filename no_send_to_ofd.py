#!/usr/bin/env python3

import psycopg2
import paramiko
import requests
import datetime
from datetime import timedelta
import re


def call_diag(cursor, fiscal_number):
    cursor.execute(f"select lastline, status from kkt_diag kd where register_number_kkt = '{fiscal_number}'")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print("\nРезультат обработки последнего фискального документа =", row[0])
            print(f"Статус отправки последнего фискального документа  = {row[1]}\n")
    else:
        print('Информация в таблице diag_kkt не найдена.')


def call_kkt(cursor, reg_number, fiscal_number):
    cursor.execute(f"select factory_number_fn, activated, is_signed, end_date, locked_no_payment from kkt "
                   f"where register_number_kkt = '{reg_number}'")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(f"Заводской номер Фискального накопителя = {row[0]}.")
            print(f'Отличает ФН в базе от введенного = {"да" if row[0] != fiscal_number else "нет"}')
            print("Признак активированной ККТ =", row[1])
            print("Касса должна принимать фискальные документы =", row[2])
            print("Дата окончания обслуживания ККТ =", row[3])
            print(f"Блокировка ККТ в связи с не оплатой  = {row[4]}\n")
    else:
        print('Информация в таблице kkt не найдена.')
    return True if rows[0][0] != fiscal_number else False


def call_stats_by_kkt(cursor, reg_number, fiscal_number):
    cursor.execute(f"select to_timestamp(last_date_time), ranges from stats.by_kkt bk "
                   f"where kkt_reg_id = '{reg_number}' and fs_id = '{fiscal_number}'")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(f"Номера ФД документов от и до = {row[1]}.")
            print(f"Максимальная Дата и время = {row[0]}\n")
    else:
        print('Информация в таблице stats.by_kkt не найдена.')


def call_replace_fn(cursor, flag, reg_number):
    if flag:
        cursor.execute(f"select old_fn, new_fn, date, type from replaced_fn_kkt rfk "
                       f"inner join kkt k on k.id = rfk.kkt_id "
                       f"where k.register_number_kkt = '{reg_number}'")
        rows = cursor.fetchall()
        if rows:
            print('Старый номер фискального накопителя\tНовый номер фискального накопителя\tДата замены\tТип замены')
            for row in rows:
                print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}")
        else:
            print('Информация в таблице stats.by_kkt не найдена.')


def check_elastic(reg_num, fiscal_num):
    login, password, host, port, _ = take_properties('elastic')
    headers = {
        'Content-Type': 'application/json',
    }
    params = (
        ('pretty', ''),
    )

    data = '{"size" : 1, "query" : { "bool" : {"must" : [' \
           '{"term" : {"requestmessage.fiscalDriveNumber.raw" : "%s"}}, ' \
           '{"term" : {"requestmessage.kktRegId.raw" : "%s"}}]}},' \
           '"sort" : {"requestmessage.dateTime" : {"order": "desc"}}}' % (fiscal_num, reg_num)

    response = requests.post(f'http://{host}:{port}/receipt.*/_search', headers=headers, params=params, data=data,
                             auth=(login, password))
    response_json = response.json()['hits']['hits'][0]['_source']
    time = datetime.datetime.utcfromtimestamp(response_json['responsemessage']['dateTime'])+timedelta(hours=3)
    doc_id = response_json['responsemessage']['fiscalDocumentNumber']
    print(f'Документ в эластике был получен в {time}')
    print(f'Номер последнего документа в эластике = {doc_id}\n')
    return time, response_json['meta']['uuid']


def take_properties(type_auth):
    with open('properties', 'r') as prop:
        for line in prop:
            if line.strip().startswith(f'user_{type_auth}'):
                user = line.strip().split('=')[1]
            elif line.strip().startswith(f'host_{type_auth}'):
                host = line.strip().split('=')[1]
            elif line.strip().startswith(f'port_{type_auth}'):
                port = int(line.strip().split('=')[1])
            elif line.strip().startswith(f'password_{type_auth}'):
                password = line.strip().split('=')[1]
            elif line.strip().startswith('database'):
                database = line.split('=')[1].strip()
    return user, password, host, port, database


def connect_sql():
    user, password, host, port, database = take_properties('db')
    connect = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
    )
    return connect


def connect_to_ssh(cmd, name_log, fn):
    print(f'Выполняется греп документа {name_log} по ФН {fn}')

    login, password, host, port, _ = take_properties('server')

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=login, port=port, password=password)
    stdin, stdout, stderr = client.exec_command(
        f'{cmd} {fn} /var/log/prom/prom-kis/{name_log}')
    data, error = stdout.read().decode('utf-8').strip().split('\n'), stderr.read().decode('utf-8').strip().split('\n')
    client.close()
    return data, error


def get_cmd_log(date_low):
    if datetime.datetime.now().date() == date_low.date():
        cmd_grep = 'grep'
        name_log = f'argentum_prom-kis_{date_low.strftime("%Y_%m_%d")}.log'
    else:
        cmd_grep = 'zgrep'
        next_date = date_low + timedelta(days=1)
        name_log = f'argentum_prom-kis_{date_low.strftime("%Y_%m_%d")}.log-{next_date.strftime("%Y%m%d")}.gz'
    return cmd_grep, name_log


def main():
    con = connect_sql()
    cur = con.cursor()

    rnm = input('Введите регистрационный номер ККТ\n')
    fn = input('Введите номер фискального накопителя\n')
    call_diag(cur, fn)
    flag_replace_fn = call_kkt(cur, rnm, fn)
    call_stats_by_kkt(cur, rnm, fn)
    call_replace_fn(cur, flag_replace_fn, rnm)
    time_in, uuid = check_elastic(rnm, fn)
    logs, errors = connect_to_ssh(*get_cmd_log(time_in), fn)
    flag_uuid = 0
    with open(f'{rnm}_{fn}.txt', 'w') as doc:
        if '' not in errors:
            for er in errors:
                doc.write(f'{er}\n')
        if '' not in logs:
            for line in logs:
                if re.search(uuid, line):
                    flag_uuid = 1
                if flag_uuid == 1:
                    doc.write(f'{line}\n')
        else:
            doc.write(f'Информации по {fn} не найдена\n')
    print(f'Лог записан в файл {rnm}_{fn}.txt\nЧао')
    con.close()


if __name__ == '__main__':
    main()