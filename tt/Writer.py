#! -*- coding: utf-8 -*-

import os
import sys
import time
import traceback

class TTWriter(object):
    def __init__(self, _L):
        self.L = _L
        self.path_ = ''
        self.fname_= ''

        # 디렉토리 관리를 위함.
        self.date_ = ''
        self.t_file_ = ''

    def Init(self, _path, _fname):
        self.path_ = _path
        self.fname_= _fname

    def Write(self, event_date, event_time, log_code, msg):

        now_str = time.strftime("%Y%m%d%H%M%S")

        if long(now_str) - long(event_date + event_time) > 10 * 60:
            self.L.log(1, 'WRN| delay write {0}'.format(msg))
            return

        self.L.log(2,
            'INF| TT Write event_date {0} event_time {1} code {2}'.format(event_date,
                                                                            event_time,
                                                                            log_code))

        self.getFname(now_str[:8])

        try:
            f = open(self.t_file_, 'a+')
            f.write(msg)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))
        finally:
            if f : f.close()

    def getFname(self, _date):

        if self.date_ != _date:
            t_dir = os.path.join(self.path_, _date)

            if not os.path.exists(t_dir):
                os.mkdir(t_dir)
            elif os.path.isfile(t_dir):
                os.unlink(t_dir)
                os.mkdir(t_dir)
            else:
                pass

            self.t_file_ = os.path.join(t_dir, self.fname_)
            self.date_ = _date

class NIMPWriter(object):
    def __init__(self, _L):
        self.L = _L
        self.path_ = ''
        self.fname_= ''

        self.dict_suffix = {'A':'.alm', 'S':'.sts', 'F':'.flt', 'M':'.mmi', 'T':'.trc'}

        # 디렉토리 관리를 위함.
        self.date_ = ''
        self.t_file_ = ''

    def Init(self, _path, _fname):
        self.path_ = _path
        self.fname_= _fname

    def Write(self, event_date, event_time, alias_code, msg):

        now_str = time.strftime("%Y%m%d%H%M%S")

        if long(now_str) - long(event_date + event_time) > 10 * 60:
            self.L.log(1, 'WRN| delay write {0}'.format(msg))
            return

        self.L.log(2,
            'INF| NIMP Write event_date {0} event_time {1} code {2}'.format(event_date,
                                                                            event_time,
                                                                            alias_code))

        self.getFname(now_str[:8])

        try:
            f = open(self.t_file_+self.dict_suffix[alias_code[0]], 'a+')
            f.write(msg)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))
        finally:
            if f : f.close()

    def getFname(self, _date):

        if self.date_ != _date:
            fname = _date + '_' + self.fname_
            t_dir = os.path.join(self.path_, fname)
            self.t_file_ = os.path.join(t_dir, t_dir)
            self.date_ = _date