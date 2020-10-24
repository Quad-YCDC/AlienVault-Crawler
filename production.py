# -*- coding: utf-8 -*-
from OTXv2 import OTXv2
import IndicatorTypes
import os, json, requests, psycopg2, time
from psycopg2 import pool
from datetime import datetime
from pytz import timezone
from tqdm import tqdm
from config import config
from termcolor import cprint

DateFormat = '%Y-%m-%d %H:%M:%S'


class GetApi:
    otx = OTXv2(
        'd208825926256517b037657addb90894cf4b663c8ba9651a67d44493334a94a4')
    dbs = otx.getall(limit=1, max_page=1)


class ConnectionDB:
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        threaded_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **params)
        if (conn and threaded_pool):
            print("Database 연결 성공")
            pool_conn = threaded_pool.getconn()
    except (Exception, psycopg2.DatabaseError) as error:
        print("PostgreSQL 연결 에러", error)


class Service:
    def reputation_service(id):
        ConnectionDB.cur.execute(
            "SELECT service_name from reputation_service where service_name = %s",
            (id, ))
        return ConnectionDB.cur.fetchone() is not None

    def ServiceIdx(idx):
        ConnectionDB.cur.execute(
            "SELECT id from reputation_service where service_name = %s",
            (idx, ))
        return ConnectionDB.cur.fetchone()

    def EnableService():
        try:
            if (Service.reputation_service('AlienVault')):
                cprint('AlienVault 서비스가 활성화되었습니다.', 'grey', 'on_green')
            else:
                print('AlienVault 서비스가 존재하지 않습니다. 생성을 시작합니다.')
                ConnectionDB.cur.execute(
                    "INSERT INTO reputation_service (id, service_name) VALUES (default, %s)",
                    ('AlienVault', ))
                ConnectionDB.conn.commit()
        except:
            print('Error')


class AuditLog:
    def audit_log_start():
        print('AlienVault 수집 시작')
        ConnectionDB.cur.execute(
            "INSERT INTO reputation_audit (id, audit_log, log_date) VALUES (default, %s, %s)",
            ('AlienVault 수집 시작', datetime.now(
                timezone('Asia/Seoul')).strftime(DateFormat)))
        ConnectionDB.conn.commit()

    def audit_log_end():
        print('AlienVault 수집 종료')
        ConnectionDB.cur.execute(
            "INSERT INTO reputation_audit (id, audit_log, log_date) VALUES (default, %s, %s)",
            ('AlienVault 수집 종료', datetime.now(
                timezone('Asia/Seoul')).strftime(DateFormat)))
        ConnectionDB.conn.commit()


class Duplication:
    def duplication_remove():
        ConnectionDB.cur.execute(
            "DELETE FROM reputation_data WHERE id in (SELECT id FROM (SELECT id, row_number() OVER (PARTITION BY indicator ORDER BY id) as row_num FROM reputation_data) a WHERE a.row_num > 1);"
        )

        return ConnectionDB.cur.fetchone


class Indicators:
    def reputation_indicator(indicator_name):
        ConnectionDB.cur.execute(
            "SELECT indicator_name from reputation_indicator where indicator_name = %s",
            (indicator_name, ))
        return ConnectionDB.cur.fetchone()

    def idx_exists(name):
        ConnectionDB.cur.execute(
            "SELECT id FROM reputation_indicator WHERE indicator_name = %s",
            (name, ))
        return ConnectionDB.cur.fetchone()

    AuditLog.audit_log_start()
    Service.EnableService()
    for i in tqdm((GetApi.dbs), ncols=100):
        indicator_id = i['id']
        indicator_tags = i['tags']
        indicator_modified = i['modified']
        indicator_name = i['name']
        indicator_revision = i['revision']
        indicator_desc = i['description']
        indicators = i['indicators']
        cprint('\nName: %s' % i['name'], 'green')
        cprint('Desc: %s' % indicator_desc, 'yellow')
        cprint('Revision: %s' % i['revision'], 'cyan')
        cprint('Tags: %s\033[0m' % i['tags'], 'blue')
        cprint('-' * 100, 'magenta')
        for idx, j in enumerate(indicators, 1):
            try:
                if (reputation_indicator(j['type'])):
                    cprint('PASS', 'green')
                else:
                    cprint('새로운 타입 발견', 'white', 'on_red')
                    ConnectionDB.cur.execute(
                        "INSERT INTO reputation_indicator (id, indicator_name) values (default, %s)",
                        (j['type'], ))
                    ConnectionDB.conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            Date = datetime.now(timezone('Asia/Seoul')).strftime(DateFormat)
            print('ID: %d' % j['id'])
            cprint('Indicator: %s' % j['indicator'], 'red')
            cprint('Type: %s' % j['type'], 'red')
            print('Created: %s' % str(j['created'].replace('T', ' ')))
            print('Registed: %s' % Date)
            cprint('=' * 100, 'cyan')
            time.sleep(0.5)
            ConnectionDB.cur.execute(
                "INSERT INTO reputation_data (id, service, indicator_type, indicator, reg_date, cre_date) values (default, %s, %s, %s, %s, %s)",
                (Service.ServiceIdx('AlienVault'), idx_exists(
                    j['type']), j['indicator'], Date, j['created']))
            ConnectionDB.conn.commit()
            Duplication.duplication_remove()


AuditLog.audit_log_end()
ConnectionDB.cur.close()
ConnectionDB.conn.close()
ConnectionDB.threaded_pool.putconn(ConnectionDB.pool_conn)
print('\033[93m작업 종료\033[00m')
