#! -*- coding: utf-8 -*-

import os
import sys
import ConfigParser
import time
import traceback
#import cx_Oracle
import pyodbc
import signal
import string
from datetime import datetime
from multiprocessing import Process

from common import Define
from common import CommLog
from common import Delete


class DB(object):
    def __init__(self):
        self.cnxn = None
        self.cursor = None

    def close(self):
        if not self.cursor:
            self.cursor.close()
            self.cursor = None

        if not self.cnxn:
            self.cnxn.close()
            self.cnxn = None

    def execute(self, sql, commit=True):
        if not self.cnxn:
            return False

        if not self.cursor:
            self.cursor = self.cnxn.cursor()

        self.cursor.execute(sql)

        if commit:
            self.cnxn.commit()

        return True

    def query(self, sql):
        if not self.cnxn:
            return []

        if not self.cursor:
            self.cursor = self.cnxn.cursor()

        self.cursor.execute(sql)

        return self.cursor.fetchall()


class ADB(DB):
    def __init__(self, p_dsn, s_dsn):
        DB.__init__(self)

        self.p_dsn = p_dsn
        self.s_dsn = s_dsn

    def init(self, L):

        if self.cnxn:
            return True

        try:
            self.cnxn = pyodbc.connect(dsn=self.p_dsn)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))
            L.log(0, 'WRN| try to connect S')

        try:
            self.cnxn = pyodbc.connect(dsn=self.s_dsn)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))
            L.log(0, 'WRN| altibase connect fail')
            return False

        L.log(0, 'INF| altibase db connect success')
        return True

'''
class ODB(DB):
    def __init__(self, ip, port, sid, id, pw):
        DB.__init__(self)

        self.ip   = ip
        self.port = port
        self.sid  = sid
        self.id   = id
        self.pw   = pw

    def init(self, L):

        if self.cnxn:
            return True

        try:
            self.cnxn = cx_Oracle('{0}/{1}@{2}:{3}/{4}'.format(self.id,
                                                             self.pw,
                                                             self.ip,
                                                             self.port,
                                                             self.sid))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))
            L.log(0, 'WRN| oracle connect fail')
            return False

        L.log(0, 'INF| oracle db connect success')
        return True
'''

class Job(object):
    def __init__(self, svc_name, L):
        self.name = svc_name
        self.L    = L

        self.db_name      = ''
        self.path         = ''
        self.fname_prefix = ''
        self.period       = ''

        self.sql          = ''

        self.db           = None


    def init(self, C):

        self.res_path     = C.get('Global', 'res-path')
        self.delete_period= C.get('Global', 'data-delete-period')

        self.my_name      = C.get('Global', 'my-name')
        self.my_station   = C.get('Global', 'my-station')

        self.db_name      = C.get(self.name, 'db')
        self.path         = C.get(self.name, 'data-path')
        self.fname_prefix = C.get(self.name, 'file-name-prefix')
        self.period       = C.get(self.name, 'make-file-period')

        sql_file          = C.get(self.name, 'query-file')

        with open(sql_file, 'r') as f:
            while True:
                line = f.readline()

                if not line:
                    break

                self.sql += line

        # DB 접속 합니다.
        '''
        if 'ORA' in self.db_name.upper():
            self.db = ODB(C.get(self.db_name, 'oracle-ip'),
                          C.get(self.db_name, 'oracle-port'),
                          C.get(self.db_name, 'oracle-sid'),
                          C.get(self.db_name, 'oracle-id'),
                          C.get(self.db_name, 'oracle-pw'))
        
        else:
            self.db = ADB(C.get(self.db_name, 'pdb-p-dsn'),
                          C.get(self.db_name, 'pdb-s-dsn'))
        '''

        self.db = ADB(C.get(self.db_name, 'pdb-p-dsn'),
                      C.get(self.db_name, 'pdb-s-dsn'))

    def work(self, d, t):

        # 시간 체크는 내가 합니다. ㅠ.ㅜ
        # file 을 읽어서 내가 작업해야 할 시간인지 확인 합니다.

        b_date = ''
        b_time = ''
        a_date  = ''
        a_time  = ''

        try:
            (b_date, b_time, a_date, a_time) = self.is_next_time(d, t)
        except TypeError:
            return


        self.L.log(0, 
            'DEG| svc {0} bd {1} bt {2} ad {3} at {4}'.format(self.name, b_date.ljust(8, '0'), b_time.ljust(6, '0'), a_date.ljust(8, '0'), a_time.ljust(6, '0'))) 

        sql = self.sql.format(BEFORE_DATE=b_date.ljust(8, '0'),
                              BEFORE_TIME=b_time.ljust(6, '0'),
                              AFTER_DATE =a_date.ljust(8, '0'),
                              AFTER_TIME =a_time.ljust(6, '0'))

        #self.L.log(2, 'DEG| {0}'.format(sql))

        self.db.init(self.L)
        ret = self.db.query(sql)
        self.db.close()

        if not ret:
            self.L.log(0, 
                'WRN| query result is null [{0} {1}] [{2}]'.format(a_date, a_time, sql)) 
            return 

        list_ret = []
        list_ret.append(self.my_station)
        list_ret.append(self.my_name)
        list_ret.append(a_date.ljust(8, '0'))
        #list_ret.append(self.name)
        list_ret.append(a_time[:2])
        list_ret.append(a_time[2:4])

        fname = self.fname_prefix + '_' + a_date + '_' + a_time[:2] + '_' + a_time[2:4] + '.DAT'
        # 결과를 출력 합니다.

        with open(os.path.join(self.path, fname), 'a+') as f:
            for row in ret:
                # 중간에 끼어 넣을려구요..
                #f.write(','.join(list_ret[:2] + row[0][:-1].split() + list_ret[2:] + list(row[1:])) + '\n')

                if 'MCC' in self.fname_prefix:
                    try:
                        int(row[1])
                    except ValueError:
                        self.L.log(0, '#{0} {1} {2}'.format(row[1], type(row[1]), len(row[1])))
                        continue

                f.write(','.join(list_ret[:2] + row[0][:-1].split() + list_ret[2:] + list(row[1:])) + '\n')

                #self.L.log(0, 
                #    'INF| --- {0}'.format(list(row)))
                #self.L.log(0, 
                #    'INF| --- [{0}] [{1}]'.format(row[0][:-1], list(row[1:])))
                #self.L.log(0, 
                #    'INF| --- {0}'.format(','.join(list_ret[:2] + row[0][:-1].split() + list_ret[2:] + list(row[1:]))))

        self.save_next_time(a_date, a_time, int(self.period) * 60)

    def get_before_date(self, d, t, period):

        now       = datetime.strptime(d+t, '%Y%m%d%H%M')
        stamp     = time.mktime(now.timetuple()) - (period * 60)
        next_str  =  datetime.fromtimestamp(stamp).strftime('%Y%m%d%H%M')
        return (next_str[:8], next_str[8:12])

    def is_next_time(self, date, time):

        fd        = None
        next_date = ''

        try:
            fd = open(os.path.join(self.res_path, self.name), 'r')
            next_date = fd.readline()
        except IOError :
            pass
        else:
            if fd:
                fd.close()
                fd = None

        if not next_date:
            self.save_next_time(date, time, int(self.period) * 60)
            return

        # next_date - YYYYMMDD HHmm
        next_date_list = next_date.split()

        if date < next_date_list[0]:
            return

        '''
        if date == next_date_list[0] and time < next_date_list[1]:
            return

        self.L.log(0, 
            'DEG| ## {0} {1} {2} {3}'.format(time, 
                                             next_date_list[1], 
                                             self.diff(time, next_date_list[1]),
                                             int(self.period)))
        '''

        if self.diff(date, time, next_date_list[0], next_date_list[1]) < int(self.period):
            return

        (b_date, b_time) = self.get_before_date(next_date_list[0], next_date_list[1], int(self.period))
        return (b_date, b_time, next_date_list[0], next_date_list[1])

    def diff(self, now_d, now_t, next_d, next_t):

        now_min = 0

        if now_d > next_d:
            now_min  = 60 * 24 + int(now_t[:2])  * 60 + int(now_t[2:4])
        else:
            now_min  = int(now_t[:2])  * 60 + int(now_t[2:4])

        next_min = int(next_t[:2]) * 60 + int(next_t[2:4])

        return now_min - next_min

    def save_next_time(self, d, t, period):
        # 20150930 2359
        # 20150930 0001
        # 20150930 2355

        # 현재보다 가장 가까운 작업 시간을 찍어야 합니다.
        now       = datetime.strptime(d+t, '%Y%m%d%H%M')
        stamp     = time.mktime(now.timetuple())
        new_stamp = ( int(stamp) / period + 1 ) * period
        next_d    =  datetime.fromtimestamp(new_stamp)

        with open(os.path.join(self.res_path, self.name), 'w') as f:
            f.write(next_d.strftime('%Y%m%d %H%M'))
            self.L.log(0, 
                'INF| {0} next sched time now:{1}{2}, next:{3}'.format(self.name,
                                                                       d,
                                                                       t,
                                                                       next_d.strftime('%Y%m%d %H%M')))

class Worker(object):
    def __init__(self):
        self.dict       = {}
        self.L          = None

        self.term       = False
        self.process    = None

    def regist_signal(self):
        signal.signal(signal.SIGTERM, self.handler)

    def handler(self, signum, frame):

        if signum == signal.SIGTERM:
            self.L.log(0, 'WRN| received term signal')
            self.term = True
        else:
            self.L.log(0, 'WRN| received {0} signal'.format(signum))
            pass

    def is_alive(self):
        if not self.process:
            return False

        return self.process.is_alive()

    def join(self):
        if not self.process:
            return None

        return self.process.join()

    def Init(self):

        C = ConfigParser.ConfigParser()
        #C.read(os.path.join(os.getenv('HOME'), 'TT', 'PyTT', 'conf', 'Eureka.cfg'))
        C.read(os.path.join(Define.MACRO['config_path'], 'Eureka.cfg'))

        self.dict['LOG_LEVEL']              = C.get('Global', 'log-level')
        self.dict['LOG_BASE_PATH']          = C.get('Global', 'log-path')

        self.L = CommLog.Log(int(self.dict['LOG_LEVEL']))
        self.L.init(self.dict['LOG_BASE_PATH'], 'worker.log', False)

        # svc 를 읽어야 합니다.
        self.dict['SERVICE_LIST']           = C.get('SVC', 'list').replace(' ', '').split(',')
        self.dict['JOB_OBJECT_LIST']        = []

        for svc_name in self.dict['SERVICE_LIST']:
            self.dict['JOB_OBJECT_LIST'].append(Job(svc_name, self.L))

        for job in self.dict['JOB_OBJECT_LIST']:
            job.init(C)

        return True

    def Do(self):

        self.regist_signal()

        if not self.Init():
            self.L.log(0, 'ERR| Init fail')
            return 

        self.L.log(0, '================ START ================')

        data_delete = Delete.Delete()

        while not self.term and os.getppid() != 1:
            self.L.log(0, 'INF| try to work {0}'.format(self.dict['SERVICE_LIST']))
            now_str = time.strftime('%Y%m%d%H%M')

            for job in self.dict['JOB_OBJECT_LIST']:
                job.work(now_str[:8], now_str[8:12])
                data_delete.delete_file(job.path, job.period, self.L)

            time.sleep(60)

        self.L.log(0, '================ END ================')

class Service(object):
    def __init__(self, name):

        self.name    = name[name.rfind(os.sep)+1:name.rfind('.py')]
        self.term    = False

        self.dict    = {}
        self.L       = None
        self.db      = None

    def regist_signal(self):
        signal.signal(signal.SIGTERM, self.handler)

    def handler(self, signum, frame):

        if signum == signal.SIGTERM:
            self.L.log(0, 'WRN| received term signal')
            self.term = True
        else:
            self.L.log(0, 'WRN| received {0} signal'.format(signum))
            pass

    def run_process(self, o):
        p = Process(target=o.Do)
        p.start()
        o.process = p

        self.L.log(0, 'INF| Process starts')
        return


    def daemon(self, _path, arg_name):

        # is duplicated
        ret = True
        try:
            pid = ''
            with open(os.path.join(_path, self.name), 'r') as f:
                pid = f.readline()

            pid = pid.strip()

            line = ''
            with open(os.path.join('/proc', pid, 'cmdline'), 'r') as f:
                line = f.readline()

            if arg_name in line:
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

        with open(os.path.join(_path, self.name), 'w') as f:
            f.write('{0}'.format(os.getpid()))

        return False

    def Init(self):
        # Config 를 읽어요..
        C = ConfigParser.ConfigParser()
        #C.read(os.path.join(os.getenv('HOME'), 'TT', 'PyTT', 'conf', 'Eureka.cfg'))
        C.read(os.path.join(Define.MACRO['config_path'], 'Eureka.cfg'))

        self.dict['LOG_LEVEL']              = C.get('Global', 'log-level')
        self.dict['LOG_BASE_PATH']          = C.get('Global', 'log-path')
        self.dict['RES_BASE_PATH']          = C.get('Global', 'res-path')
        self.dict['LOG_DELETE_PERIOD']      = C.get('Global', 'log-delete-period')

        if self.daemon(self.dict['RES_BASE_PATH'], self.name):
            print 'process alreay running..'
            sys.exit(0)

        self.L = CommLog.Log(int(self.dict['LOG_LEVEL']))
        self.L.init(self.dict['LOG_BASE_PATH'], '{0}.log'.format(self.name), False)
        self.L.log(0, '================ Start ================')

        self.L.log(0,
            'INF| log-level:{0} log-path:{1}'.format(self.dict['LOG_LEVEL'],
                                                     self.dict['LOG_BASE_PATH']))
        self.L.log(0, 'INF| res-data-path:{0}'.format(self.dict['RES_BASE_PATH']))


    def Do(self):

        self.regist_signal()

        log_delete  = Delete.Delete()

        w = Worker()
        if not w.Init():
            self.L.log(0, 'ERR| worker init fail')
            return

        self.run_process(w)

        try:
            while not self.term:
                if not w.is_alive():
                    self.L.log(0, 'INF| Process Down')
                    self.run_process(w)

                time.sleep(10)

                log_delete.delete_dir(self.dict['LOG_BASE_PATH'], 
                                      self.dict['LOG_DELETE_PERIOD'], 
                                      self.L)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))

        self.L.log(0, 'WRN| out of service')

        w.process.terminate()
        w.join()

        self.L.log(0, '---------------- END ----------------')
        sys.exit(0)


if __name__ == '__main__':

    if len(sys.argv) != 1:
        print 'ERR| invalid argument'
        print 'ex) python EK.py'
        sys.exit(0)

    instance = Service(sys.argv[0])

    instance.Init()
    instance.Do()

