# -*- coding: utf-8 -*-
from OTXv2 import OTXv2
import IndicatorTypes
import os, json, requests, psycopg2, time
from datetime import datetime
from pytz import timezone
from tqdm import tqdm
from tqdm import tqdm_notebook

otx = OTXv2('d208825926256517b037657addb90894cf4b663c8ba9651a67d44493334a94a4')
dbs = otx.getall(limit=1, max_page=3)
DateFormat = '%Y-%m-%d %H:%M:%S'

conn = psycopg2.connect(host='localhost',
                        dbname='postgres',
                        user='postgres',
                        password='1234')

try:
    if conn:
        print("Database 연결 성공")
        cur = conn.cursor()
    else:
        print('Database 연결 실패')
except:
    print("이외 Error")


def reputation_service(id):
    cur.execute(
        "SELECT service_name from reputation_service where service_name = %s",
        (id, ))
    return cur.fetchone() is not None


def reputation_indicator(indicator_name):
    cur.execute(
        "SELECT indicator_name from reputation_indicator where indicator_name = %s",
        (indicator_name, ))
    return cur.fetchone() is not None


def reputation_data_idx_check(id):
    cur.execute("SELECT id from reputation_data where id = %s", (id, ))
    return cur.fetchone() is not None


def reputation_audit(id):
    cur.execute("SELECT id FROM reputation_audit WHERE id = %s", (id, ))
    return cur.fetchone() is not None


def idx_exists(name):
    cur.execute(
        "SELECT id FROM reputation_indicator WHERE indicator_name = %s",
        (name, ))
    return cur.fetchone()


def duplication_remove():
    cur.execute(
        "DELETE FROM reputation_data WHERE id in (SELECT id FROM (SELECT id, row_number() OVER (PARTITION BY indicator ORDER BY id) as row_num FROM reputation_data) a WHERE a.row_num > 1);"
    )
    return cur.fetchone is not None


try:
    if (reputation_service('AlienVault')):
        print('AlienVault 서비스가 활성화되었습니다.')
    else:
        print('AlienVault 서비스가 존재하지 않습니다. 생성을 시작합니다.')
        cur.execute(
            "INSERT INTO reputation_service (id, service_name) VALUES (%s, %s)",
            (1, 'AlienVault'))
        conn.commit()
except:
    print('Error')


def audit_log_start():
    global key
    key = 1
    while True:
        if (reputation_audit(key)):
            pass
        else:
            print('AlienVault 수집 시작')
            cur.execute(
                "INSERT INTO reputation_audit (id, audit_log, log_date) VALUES (%s, %s, %s)",
                (key, 'AlienVault 수집 시작', datetime.now(
                    timezone('Asia/Seoul')).strftime(DateFormat)))
            conn.commit()
            break
        key += 1
    return


def audit_log_end():
    global key
    while True:
        if (reputation_audit(key)):
            pass
        else:
            print('AlienVault 수집 종료')
            cur.execute(
                "INSERT INTO reputation_audit (id, audit_log, log_date) VALUES (%s, %s, %s)",
                (key, 'AlienVault 수집 종료', datetime.now(
                    timezone('Asia/Seoul')).strftime(DateFormat)))
            conn.commit()
            break
        key += 1
    return


def reputation_data():
    global indicator_data_idx
    indicator_data_idx = 1
    while True:
        if (reputation_data_idx_check(indicator_data_idx)):
            pass
        else:
            cur.execute(
                "INSERT INTO reputation_data (id, service, indicator_type, indicator, reg_date, cre_date) values (%s, %s, %s, %s, %s, %s)",
                (indicator_data_idx, 1, idx_exists(
                    j['type'])[0], j['indicator'], Date, j['created']))
            conn.commit()
            break
        indicator_data_idx += 1
    return


audit_log_start()
indicator_type_idx = 1
indicator_data_idx = 1
for i in tqdm((dbs), ncols=100):
    # print(json.dumps(i['indicators'], indent=4, sort_keys=True))
    indicator_id = i['id']
    indicator_tags = i['tags']
    indicator_modified = i['modified']
    indicator_name = i['name']
    indicator_revision = i['revision']
    indicator_desc = i['description']
    indicators = i['indicators']
    print('\n\033[95mName: %s' % i['name'])
    print('\033[96mDesc: %s' % indicator_desc)
    print('\033[91mRevision: %s' % i['revision'])
    print('\033[94mTags: %s\033[0m' % i['tags'])
    print('\033[90m---------------\033[0m')
    for idx, j in enumerate(indicators, 1):
        if (reputation_indicator(j['type'])):
            print('\033[92mPASS\033[0m')
        else:
            print('\033[41m새로운 타입 발견\033[0m')
            cur.execute(
                "INSERT INTO reputation_indicator (id, indicator_name) values (%s, %s)",
                (indicator_type_idx, j['type']))
            indicator_type_idx += 1
            conn.commit()
        Date = datetime.now(timezone('Asia/Seoul')).strftime(DateFormat)
        reputation_data()
        print('ID: %d' % j['id'])
        print('Indicator: %s' % j['indicator'])
        print('Type: %s' % j['type'])
        print('Created: %s' % str(j['created'][2:].replace('T', ' ')))
        print('Registed: %s' % Date)
        print('=========================')
        time.sleep(0.1)

duplication_remove()
audit_log_end()
conn.commit()
cur.close()
conn.close()
print('\033[93m작업 종료\033[00m')