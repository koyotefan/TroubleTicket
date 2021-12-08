#! -*- coding: utf-8 -*-

import os
import sys
import time
import struct
import string
import pyodbc
import traceback
from datetime import datetime

LOG_LEVEL = {'0':'NON', '1':'NOR', '2':'MIN', '3':'MAJ', '4':'CRI'}

fmt_header  = '>2s2s2s4s5s'

fmt_fm_pg114 = '>8s6s2s8s8s8s5sc200scc8s20s100s18s'
fmt_fm_pg113 = '>8s6s2s8s8s8sc200scc8s20s100s18s'

fmt_ap_pg114 = '>8s6sc6s2s6s4s2s8s8s8s5sc200scc8s20s100s18s'
fmt_ap_pg113 = '>8s6sc6s2s6s4s2s8s8s8sc200scc8s20s100s18s'

fmt_connect_check_req = '>8s6s20s'


class CCmsg(object):
    def __init__(self, _pkg_id):
        self.pkg_id_ = _pkg_id

    def make(self):
        # default system id  2 BYTE
        # system id          2 BYTE
        # package id         2 BYTE
        # packet type        4 BYTE - 2200 (CONNECT CHECK REQ)
        # bodysize           5 BYTE - (Body Size)
        H = struct.pack(fmt_header, '00', '00', self.pkg_id_, '2200',
                        str(struct.calcsize(fmt_connect_check_req)))

        date = time.strftime('%Y%m%d%H%M%S')

        # event date         8 BYTE
        # event time         6 BYTE
        # reserved          20 BYTE
        B = struct.pack(fmt_connect_check_req, date[:8], date[8:14], '0'.zfill(20))

        return (H, B)




class MSG(object):
    def __init__(self, _L, _pkg_id, _system_name):
        self.L = _L
        self.pkg_id_ = _pkg_id
        self.system_name_ = _system_name

        self.dict_code_ = {}

        self.list_recover_code_ = []

        self.tuple_h_ = ()

    def Init(self, _list_dsn, _column_name):

        cnxn = None
        for info in _list_dsn:
            try:
                cnxn = pyodbc.connect(dsn=info)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.L.log(0,
                    "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))
            else:
                self.L.log(0, 'INF| connect successful {0}'.format(info))
                break

        if not cnxn:
            self.L.log(0, 'ERR| cannot connect altibase DB')
            return False

        cursor = cnxn.cursor()

        sql = 'SELECT DISTINCT LOG_CODE, LOG_LEVEL, LOG_GROUP, RECOVERY_LOG_CODE, LOG_CODE_ALIAS, LOG_TITLE '
        sql += " FROM T_NFW_LOG_TEMPLATE WHERE PACKAGE_ID='{0}' AND {1}='Y'".format(self.pkg_id_, _column_name)

        try:
            cursor.execute(sql)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))

            if cursor:
                cursor.close()
            if cnxn :
                cnxn.close()

            return False

        rows = cursor.fetchall()

        self.L.log(0, 'INF| read {0} data'.format(_column_name))
        for row in rows:
            self.dict_code_[row[0]] = row
            self.list_recover_code_.append(row[2])
            self.L.log(0,
                'INF| Init CODE [{0}] LEVEL [{1}] RECOVER [{2}] ALIAS [{3}] TITLE [{4}]'.format(row[0],
                                                                                    row[1],
                                                                                    row[3],
                                                                                    row[4],
                                                                                    row[5]))

        cursor.close()
        cnxn.close()

        return True

    def Filter(self, tuple_h, B):

        # tuple_h[3]         packet type
        # PFM_LOG_EVENT      9041

        # event date         8 BYTE
        # event time         6 BYTE
        # component id       2 BYTE
        # log date           8 BYTE
        # log time           8 BYTE
        # log code           8 BYTE
        # NEW -- log alias      5 BYTE ( PG 114 )
        # log level          1 BYTE
        # log message      200 BYTE
        # log confirm flag   1 BYTE
        # voice noti flag    1 BYTE
        # recovery log code  8 BYTE
        # argments order    20 BYTE
        # arguments        100 BYTE
        # reserved          18 BYTE
        if tuple_h[3] == '9041':
            if len(B) == struct.calcsize(fmt_fm_pg114):
                tuple_b = struct.unpack(fmt_fm_pg114, B)
                if self.takeCare(tuple_b[5]):
                    self.tuple_h_ = tuple_h

                    self.event_day_        =  tuple_b[0]
                    self.event_time_       =  tuple_b[1]

                    self.log_code_         =  tuple_b[5]
                    self.log_level_        =  tuple_b[7]
                    self.log_message_      =  tuple_b[8]
                    self.log_recover_code_ =  tuple_b[9]
                    return True

                self.L.log(1,
                    'INF| dont care pfm code [{0}] time [{1}:{2}] msg [{3}]'.format(tuple_b[5],
                                                                                    tuple_b[0],
                                                                                    tuple_b[1],
                                                                                    filter(lambda x: x in string.printable, tuple_b[8])))
            elif len(B) == struct.calcsize(fmt_fm_pg113):
                tuple_b = struct.unpack(fmt_fm_pg113, B)
                if self.takeCare(tuple_b[5]):
                    self.tuple_h_ = tuple_h

                    self.event_day_        =  tuple_b[0]
                    self.event_time_       =  tuple_b[1]

                    self.log_code_         =  tuple_b[5]
                    self.log_level_        =  tuple_b[6]
                    self.log_message_      =  tuple_b[7]
                    self.log_recover_code_ =  tuple_b[8]

                    return True

                self.L.log(1,
                    'INF| dont care pfm code [{0}] time [{1}:{2}] msg [{3}]'.format(tuple_b[5],
                                                                                    tuple_b[0],
                                                                                    tuple_b[1],
                                                                                    filter(lambda x: x in string.printable, tuple_b[7])))
            else:
                self.L.log(1, 'WRN| unknown version UA_STFrameworkLogEvent packet [size:{0}'.format(len(B)))

            return False

        # tuple_h[3]       packet type
        # AP_LOG_EVENT     9042

        # event date         8 BYTE
        # event time         6 BYTE
        # app type           1 BYTE
        # service id         6 BYTE
        # component type     2 BYTE
        # process id         6 BYTE
        # process seq        4 BYTE
        # log date           8 BYTE
        # log time           8 BYTE
        # log code           8 BYTE
        # NEW -- log alias       5 BYTE
        # log level          1 BYTE
        # log message      200 BYTE
        # log confirm flag   1 BYTE
        # voice noti flag    1 BYTE
        # recovery log code  8 BYTE
        # argments order    20 BYTE
        # arguments        100 BYTE
        # reserved          18 BYTE
        if tuple_h[3] == '9042':
            if len(B) == struct.calcsize(fmt_ap_pg114):
                tuple_b = struct.unpack(fmt_ap_pg114, B)
                if self.takeCare(tuple_b[9]):
                    self.tuple_h_ = tuple_h

                    self.event_day_        =  tuple_b[0]
                    self.event_time_       =  tuple_b[1]

                    self.log_code_         =  tuple_b[9]
                    self.log_level_        =  tuple_b[11]
                    self.log_message_      =  tuple_b[12]
                    self.log_recover_code_ =  tuple_b[15]

                    return True

                self.L.log(1,
                    'INF| dont care app code [{0}] time [{1}:{2}] msg [{3}]'.format(tuple_b[9],
                                                                                    tuple_b[0],
                                                                                    tuple_b[1],
                                                                                    tuple_b[12]))
                return False

            elif len(B) == struct.calcsize(fmt_ap_pg113):
                tuple_b = struct.unpack(fmt_ap_pg113, B)
                if self.takeCare(tuple_b[9]):
                    self.tuple_h_ = tuple_h

                    self.event_day_        =  tuple_b[0]
                    self.event_time_       =  tuple_b[1]

                    self.log_code_         =  tuple_b[9]
                    self.log_level_        =  tuple_b[10]
                    self.log_message_      =  tuple_b[11]
                    self.log_recover_code_ =  tuple_b[14]

                    return True

                self.L.log(1,
                    'INF| dont care app code [{0}] time [{1}:{2}] msg [{3}]'.format(tuple_b[9],
                                                                                    tuple_b[0],
                                                                                    tuple_b[1],
                                                                                    tuple_b[11]))
                return False
            else:
                self.L.log(1, 'WRN| unknown version UA_STAppLogEvent packet [size:{0}'.format(len(B)))


        return False

    def takeCare(self, code):

        try:
            data = self.dict_code_[code]
        except KeyError:
            return False
        else:
            return True

    def Transform(self, writer):

        weekday    = datetime.strptime(self.event_day_, '%Y%m%d').strftime('%a')

        row = self.dict_code_[self.log_code_]

        db_alias         = row[4]
        db_title         = row[5]
        db_recovery_code = row[3]

        form =  ' %s %s-%s-%s %s %s:%s:%s\n' % (self.system_name_,
                                                self.event_day_[:4],
                                                self.event_day_[4:6],
                                                self.event_day_[6:],
                                                weekday,
                                                self.event_time_[:2],
                                                self.event_time_[2:4],
                                                self.event_time_[4:])
        # DB LOG_CODE_ALIAS, DB CODE_LEVEL, DB LOG_TITLE
        form += '%-6s %s %s\n' % (db_alias, LOG_LEVEL[self.log_level_], db_title)
        form += ' LOCALTION : %s\n' % self.system_name_
        form += ' TYPE : %s\n' % ('Recover' if self.log_code_ in self.list_recover_code_ else 'Occure')
        form += ' INFO :\n'
        # log message
        form += '    %s\n' % filter(lambda x: x in string.printable, self.log_message_)
        form += ';\n'

        writer.Write(self.event_day_, self.event_time_, db_alias, form)


class NIMPmsg(MSG):
    def __init__(self, _L, _pkg_id, _system_name):
        MSG.__init__(self, _L, _pkg_id, _system_name)
        self.L = _L
        self.pkg_id_ = _pkg_id
        self.system_name_ = _system_name

    def Init(self, _list_dsn):
        return MSG.Init(self, _list_dsn, 'NIMP_FLAG')


class TTmsg(MSG):
    def __init__(self, _L, _pkg_id, _system_name):
        MSG.__init__(self, _L, _pkg_id, _system_name)
        self.L = _L
        self.pkg_id_ = _pkg_id
        self.system_name_ = _system_name

    def Init(self, _list_dsn):
        return MSG.Init(self, _list_dsn, 'TT_FLAG')

