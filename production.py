# -*- coding: utf-8 -*-
from modules import *


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

    while 1:
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
                Date = datetime.now(
                    timezone('Asia/Seoul')).strftime(DateFormat)
                print('ID: %d' % j['id'])
                cprint('Indicator: %s' % j['indicator'], 'red')
                cprint('Type: %s' % j['type'], 'red')
                print('Created: %s' % str(j['created'].replace('T', ' ')))
                print('Registed: %s' % Date)
                cprint('=' * 100, 'cyan')
                # time.sleep(0.5)
                ConnectionDB.cur.execute(
                    "INSERT INTO reputation_data (id, service, indicator_type, indicator, reg_date, cre_date) values (default, %s, %s, %s, %s, %s)",
                    (Service.ServiceIdx('AlienVault'), idx_exists(
                        j['type']), j['indicator'], Date, j['created']))
                ConnectionDB.conn.commit()
                Duplication.duplication_remove()

        AuditLog.audit_log_end()
        # ConnectionDB.cur.close()
        # ConnectionDB.conn.close()
        # ConnectionDB.threaded_pool.putconn(ConnectionDB.pool_conn)
        print('\033[93m작업 종료\033[00m')
