#! -*- coding: utf-8 -*-

from COMMON import CommLog
import Message
import Writer

import signal
import time
import socket
import traceback
import sys
import struct
import os
import select

fmt_header = '>2s2s2s4s5s'

class Uas(object):
    def __init__(self, _L):
        self.L = _L

        self.ip_     = ''
        self.port_   = 0
        self.pkg_id_ = ''
        self.id_     = ''
        self.pw_     = ''

        self.conn_   = None
        self.last_try_connect_timer_ = 0

        self.last_msg_time_  = 0

    def Init(self, _ip, _port, _pkg_id, _id, _pw):
        self.ip_     = _ip
        self.port_   = int(_port)
        self.pkg_id_ = _pkg_id
        self.id_     = _id
        self.pw_     = _pw

    def Retry(f):
        def redefine(*args, **kwargs):
            if args[0].conn_:
                return args[0].conn_

            now = time.time()
            if now > args[0].last_try_connect_timer_ + int(args[1]):
                args[0].last_try_connect_timer_ = now
                return f(*args, **kwargs)
            else:
                time.sleep(1)

        return redefine

    @Retry
    def Connect(self, period):
        try:
            self.conn_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.L.log(0, 'INF| try connect')

            self.conn_.connect((self.ip_, self.port_))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))
            if self.conn_:
                self.conn_.close()
                self.conn_ = None
            return None
        else:
            self.L.log(0, 'INF| connect successful')
            return self.login()

    def send_keep_alive(self):

        if not self.conn_:
            return

        now = int(time.time())
        if self.last_msg_time_ + 10 >= now:
            return

        H, B = Message.CCmsg(self.pkg_id_).make()

        try:
            self.conn_.sendall(H+B)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))

            if self.conn_:
                self.conn_.close()
                self.conn_ = None
            return

        self.last_msg_time_ = now
        self.L.log(2, 'DEG| send keep alive')
        return

    def Recv(self, size):
        data = ''
        part = None

        readn = 0
        readt = 0

        while readt < size:
            part = self.conn_.recv(size - readt)

            if not part:
                self.L.log(0, 'ERR| disconnect by peer')
                self.conn_.close()
                self.conn_ = None
                return None

            data += part
            readt += len(part)

        return data

    def RecvMsg(self):

        i, o, e = select.select([self.conn_], [], [], 1)

        if len(i) == 0 and len(o) == 0 and len(e) == 0:
            return (None, None)

        # default system id  2 BYTE
        # system id          2 BYTE
        # package id         2 BYTE
        # packet type        4 BYTE
        # bodysize           5 BYTE
        H = self.Recv(15) # 15 means header size

        if not H:
            return (None, None)

        tuple_h = struct.unpack(fmt_header, H)

        self.L.log(2, 'DEG| recved type [{0}] body size [{1}]'.format(tuple_h[3], tuple_h[4]))

        B = self.Recv(int(tuple_h[4]))

        return (tuple_h, B)

    def login(self):

        self.L.log(0, 'INF| login processing')

        # default system id  2 BYTE
        # system id          2 BYTE
        # package id         2 BYTE
        # packet type        4 BYTE - 1011 (Login Request)
        # bodysize           5 BYTE - 132 (Body Size)
        H = struct.pack(fmt_header,
            '00', '00', self.pkg_id_, '1011', '132'.zfill(5))

        # userid            20 BYTE
        # password          64 BYTE
        # client type        2 BYTE - 91, EMS 를 흉내냄.
        # relogin flag       1 BYTE - 3, 나도 모름.
        # reserved          45 BYTE
        B = struct.pack('>20s64s2s1s45s',
            self.id_, self.pw_, '91', '3', '0'.zfill(45))

        self.conn_.sendall(H+B)

        # default system id  2 BYTE
        # system id          2 BYTE
        # package id         2 BYTE
        # packet type        4 BYTE - 1100 (Ack Response)
        # bodysize           5 BYTE - 132 (Body Size)
        H = self.Recv(15) # 15 means header size

        if not H:
            return None

        tuple_h = struct.unpack(fmt_header, H)
        if tuple_h[3] != '1100':
            self.L.log(0, 'ERR| unexpected packet type [recved:{0}]'.format(tuple_h[3]))
            self.conn_.close()
            self.conn_ = None
            return None

        # event date         8 BYTE
        # event time         6 BYTE
        # ack type           2 BYTE - 00 ok, 01 nok
        B = self.Recv(int(tuple_h[4]))

        if not B:
            return None

        tuple_b = struct.unpack('>8s6s2s', B)
        self.L.log(0,
            'INF| received login ack [date:{0} time:{1} ack:{2}]'.format(tuple_b[0],
                                                                        tuple_b[1],
                                                                        tuple_b[2]))
        if tuple_b[2] != '00':
            self.L.log(0, 'ERR| received not ok ack [recved:{0}]'.format(tuple_h[2]))
            self.conn_.close()
            self.conn_ = None
            return None

        # default system id  2 BYTE
        # system id          2 BYTE
        # package id         2 BYTE
        # packet type        4 BYTE - 1021 (Login Response)
        # bodysize           5 BYTE -  132 (Body Size)
        H = self.Recv(15)

        if not H:
            return None

        tuple_h = struct.unpack(fmt_header, H)
        if tuple_h[3] != '1021':
            self.L.log(0,
                'ERR| This is not Login Response message [recved:{0}]'.format(tuple_h[3]))
            self.conn_.close()
            self.conn_ = None
            return None

        # user id                20 BYTE
        # response code           2 BYTE
        # default package id      2 BYTE
        # package id list       100 BYTE
        # app db info         12000 BYTE
        # pfm db info         12000 BYTE
        # app ftp info          500 BYTE
        # pftm ftp info         500 BYTE
        # user info             200 BYTE

        B = self.Recv(int(tuple_h[4]))

        if not B:
            return None

        tuple_b = struct.unpack('>20s2s2s100s12000s12000s500s500s200s', B)
        if tuple_b[1] != '01':
            self.L.log(0,
                'ERR| This is not Login Response success message [recved:{0}]'.format(tuple_b[1]))
            self.conn_.close()
            self.conn_ = None
            return None

        self.L.log(0, 'INF| login on successful')
        return self.conn_

class Worker(object):
    def __init__(self, _dict):
        self.dict = _dict
        self.L    = None

        self.term_ = False

        self.ip_     = ''
        self.port_   = ''
        self.pkg_id_ = ''
        self.id_     = ''
        self.pw_     = ''
        self.system_ = ''

        self.process_ = None

    def regist_signal(self):
        signal.signal(signal.SIGTERM, self.handler)

    def handler(self, signum, frame):

        if signum == signal.SIGTERM:
            self.L.log(0, 'WRN| received term signal')
            self.term_ = True
        else:
            self.L.log(0, 'WRN| received {0} signal'.format(signum))
            pass

    def is_alive(self):
        if not self.process_:
            return False

        return self.process_.is_alive()

    def join(self):
        if not self.process_:
            return None

        return self.process_.join()

    def Init(self, _list):
        self.ip_    = _list[0]
        self.port_  = _list[1]
        self.pkg_id_= _list[2]
        #self.id_    = _list[3]
        #self.pw_    = _list[4]
        self.id_    = self.dict['UAS_USER']
        self.pw_    = self.dict['UAS_PASSWORD']
        self.system_= _list[5]

        self.L = CommLog.Log(int(self.dict['LOG_LEVEL']))
        self.L.init(self.dict['LOG_BASE_PATH'], '{0}.log'.format(self.system_), False)


    def Do(self):

        self.regist_signal()

        self.L.log(0, '================ START ================')

        self.L.log(0,
            'INF| Start IP:{0}, PORT:{1}, PKG_ID:{2}, SYSTEM:{3}'.format(self.ip_,
                                                                         self.port_,
                                                                         self.pkg_id_,
                                                                         self.system_))

        uas = Uas(self.L)
        uas.Init(self.ip_, self.port_, self.pkg_id_, self.id_, self.pw_)

        tt_msg   = Message.TTmsg(self.L, self.pkg_id_, self.system_)
        tt_msg.Init([self.dict['PDB_P'], self.dict['PDB_S'], self.dict['PDB_B']])

        nimp_msg = Message.NIMPmsg(self.L, self.pkg_id_, self.system_)
        nimp_msg.Init([self.dict['PDB_P'], self.dict['PDB_S'], self.dict['PDB_B']])

        tt_writer = Writer.TTWriter(self.L)
        tt_writer.Init(self.dict['TT_DATA_BASE_PATH'], self.system_)

        nimp_writer = Writer.NIMPWriter(self.L)
        nimp_writer.Init(self.dict['NIMP_DATA_BASE_PATH'], self.system_)

        try:
            while not self.term_ and os.getppid() != 1:
                if not uas.Connect(self.dict['RECONNECT_PERIOD']):
                    continue

                uas.send_keep_alive()

                tuple_h, B = uas.RecvMsg()

                if not tuple_h or not B:
                    continue

                if tt_msg.Filter(tuple_h, B):
                    tt_msg.Transform(tt_writer)

                if nimp_msg.Filter(tuple_h, B):
                    nimp_msg.Transform(nimp_writer)

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))

        self.L.log(0, 'WRN| Terminated.. [ppid:{0}]'.format(os.getppid()))
        self.L.log(0, '---------------- END ----------------')
