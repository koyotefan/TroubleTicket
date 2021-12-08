#! -*- coding: utf-8 -*-

import os
import sys
import ConfigParser
import signal
import time
import traceback
from multiprocessing import Process

import cx_Oracle

from COMMON import CommLog
import Work
import Delete

class UasList(object):
    def __init__(self, _L, _dict):
        self.L    = _L
        self.dict = _dict

        self.list_ = []

    def get_list(self):
        return self.list_

    def get_data(self):
        # Oracle 에서 가져옵니다.

        o_id = self.dict['ORACLE_ID']
        o_pw = self.dict['ORACLE_PW']
        o_ip = self.dict['ORACLE_IP']
        o_port=self.dict['ORACLE_PORT']
        o_sid= self.dict['ORACLE_SID']

        cnxn = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(o_id,
                                                              o_pw,
                                                              o_ip,
                                                              o_port,
                                                              o_sid))

        sql = '''SELECT U.SERVER_IP, U.SERVER_PORT, U.SERVER_PACKAGE_ID, U.SERVER_USER_ID, U.SERVER_PASSWORD, S.SYSTEM_NAME
        FROM T_NFW_EMS_UAS_INFO U INNER JOIN T_NFW_EMS_SYSTEM S ON U.SERVER_SYSTEM_ID=S.SYSTEM_ID'''
        self.L.log(3, 'DEG| SQL {0}'.format(sql))

        cur = cnxn.cursor()
        cur.execute(sql)

        for result in cur:
            #s_ip   = result[0]
            #s_port = result[1]
            #s_pid  = result[2]
            #s_user = result[3]
            #s_pw   = result[4]
            #s_sysname= result[5]

            self.list_.append(result)

        cur.close()
        cnxn.close()

class Service(object):
    def __init__(self, _type):

        self.type_ = _type
        self.term_ = False

        self.dict = {}
        self.L = None

        self.worker_list_ = []  # [Worker, ]

    def regist_signal(self):
        signal.signal(signal.SIGTERM, self.handler)

    def handler(self, signum, frame):

        if signum == signal.SIGTERM:
            self.L.log(0, 'WRN| received term signal')
            self.term_ = True
        else:
            self.L.log(0, 'WRN| received {0} signal'.format(signum))
            pass

    def run_process(self, o):
        p = Process(target=o.Do)
        p.start()
        o.process_ = p

        self.L.log(0, 'INF| Process starts [{0}]'.format(o.system_))
        return

    def daemon(self, _path):

        # is duplicated
        ret = True
        try:
            pid = ''
            with open(os.path.join(_path, self.type_), 'r') as f:
                pid = f.readline()

            pid = pid.strip()

            line = ''
            with open(os.path.join('/proc', pid, 'cmdline'), 'r') as f:
                line = f.readline()

            if 'TT.py' in line:
                ret = True
            else:
                ret = False
        except:
            ret = False

        if ret:
            return True

        # daemonize

        pid = os.fork()

        if pid > 0:
            sys.exit(0)

        with open(os.path.join(_path, self.type_), 'w') as f:
            f.write('{0}'.format(os.getpid()))

        return False


    def Init(self):

        C = ConfigParser.ConfigParser()

        #C.read(os.path.join(os.getenv('HOME'), 'TT', 'conf', 'Service.cfg'))
        C.read(os.path.join(os.getenv('HOME'), 'SRC', 'TT', 'conf', 'Service.cfg'))
        self.dict['LOG_LEVEL']          = C.get('Global', 'log-level')
        self.dict['LOG_BASE_PATH']      = C.get('Global', 'log-path')
        self.dict['TT_DATA_BASE_PATH']  = C.get('Global', 'tt-data-path')
        self.dict['NIMP_DATA_BASE_PATH']= C.get('Global',  'nimp-data-path')
        self.dict['RES_BASE_PATH']      = C.get('Global', 'res-path')
        self.dict['LOG_DELETE_PERIOD']  = C.get('Global', 'log-delete-period')
        self.dict['DATA_DELETE_PERIOD'] = C.get('Global', 'data-delete-period')

        if self.daemon(self.dict['RES_BASE_PATH']):
            print 'process alreay running..'
            sys.exit(0)

        self.L = CommLog.Log(int(self.dict['LOG_LEVEL']))
        self.L.init(self.dict['LOG_BASE_PATH'], '{0}.log'.format(self.type_), False)
        self.L.log(0, '================ Start ================')

        self.L.log(0,
            'INF| log-level:{0} log-path:{1}'.format(self.dict['LOG_LEVEL'],
                                                     self.dict['LOG_BASE_PATH']))
        self.L.log(0, 'INF| tt-data-path:{0}'.format(self.dict['TT_DATA_BASE_PATH']))
        self.L.log(0, 'INF| nimp-data-path:{0}'.format(self.dict['NIMP_DATA_BASE_PATH']))
        self.L.log(0, 'INF| res-data-path:{0}'.format(self.dict['RES_BASE_PATH']))

        self.dict['RECONNECT_PERIOD'] = C.get('Global', 'reconnect-period')
        self.L.log(0, 'INF| Reconnect period {0}'.format(self.dict['RECONNECT_PERIOD']))

        self.dict['ORACLE_ID']   = C.get(self.type_, 'oracle-id')
        self.dict['ORACLE_PW']   = C.get(self.type_, 'oracle-pw')
        self.dict['ORACLE_IP']   = C.get(self.type_, 'oracle-ip')
        self.dict['ORACLE_PORT'] = C.get(self.type_, 'oracle-port')
        self.dict['ORACLE_SID']  = C.get(self.type_, 'oracle-sid')
        self.dict['UAS_USER']    = C.get(self.type_, 'uas-user')
        self.dict['UAS_PASSWORD']= C.get(self.type_, 'uas-password')
        self.dict['PDB_P']       = C.get(self.type_, 'pdb-p-dsn')
        self.dict['PDB_S']       = C.get(self.type_, 'pdb-s-dsn')
        self.dict['PDB_B']       = C.get(self.type_, 'pdb-b-dsn')

        self.L.log(0,
            'INF| DB_ID:{0} DB_IP:{1} DB_PORT:{2}'.format(self.dict['ORACLE_ID'],
                                                          self.dict['ORACLE_IP'],
                                                          self.dict['ORACLE_PORT']))
        self.L.log(0,
            'INF| UAS_USER:{0}'.format(self.dict['UAS_USER']))
        self.L.log(0,
            'INF| PDB_P:{0} PDB_S:{1} PDB_B:{2}'.format(self.dict['PDB_P'],
                                                        self.dict['PDB_S'],
                                                        self.dict['PDB_B']))



    def Do(self):

        self.regist_signal()

        delete = Delete.Delete()

        uas_list = UasList(self.L, self.dict)
        uas_list.get_data()

        for i in uas_list.get_list():
            w = Work.Worker(self.dict)
            w.Init(i)
            self.worker_list_.append(w)
            self.run_process(w)

        try:
            while not self.term_:
                # 시간 되면, 로그나 지우고,
                # 데이터 파일이나 지우고 그래요..
                for w in self.worker_list_:
                    if not w.is_alive():
                        self.L.log(0, 'INF| Process Down [{0}]'.format(w.system_))
                        self.run_process(w)

                time.sleep(10)

                delete.delete(self.dict['LOG_BASE_PATH'], self.dict['LOG_DELETE_PERIOD'], self.L)
                delete.delete(self.dict['TT_DATA_BASE_PATH'], self.dict['DATA_DELETE_PERIOD'], self.L)
                delete.deleteNIMP(self.dict['NIMP_DATA_BASE_PATH'], self.dict['DATA_DELETE_PERIOD'], self.L)

                #self.L.log(0, 'INF| main thread..')
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))

        self.L.log(0, 'WRN| out of service')

        for w in self.worker_list_:
            w.process_.terminate()
            w.join()

        self.L.log(0, '---------------- END ----------------')
        sys.exit(0)


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print 'ERR| invalid argument'
        print 'ex) python TT.py [PCRF|PG]'
        sys.exit(0)

    if 'PCRF' != sys.argv[1]  and 'PG' != sys.argv[1]:
        print 'ERR| invalid argument'
        print 'ex) python TT.py [PCRF|PG]'

    instance = Service(sys.argv[1])

    instance.Init()
    instance.Do()
