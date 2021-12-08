#! -*- coding: utf-8 -*-

import os
import sys
import traceback
import shutil
from datetime import timedelta, datetime, date


class Delete(object):
    def __init__(self):
        self.today = datetime.today().strftime('%d')


    def deleteNIMP(self, _t_dir, _period, _L):

        day = datetime.today()
        if self.today == day.strftime('%d'):
            return

        self.today = day.strftime('%d')

        yyyymmdd = day.strftime('%Y%m%d')

        files = [ f for f in os.listdir(_t_dir) if os.path.isfile(os.path.join(_t_dir, f))]

        for f in files:
            if yyyymmdd in f:
                os.remove(f)
                _L.log(1, 'INF| delete [{0}]'.format(f))

    def delete(self, _t_dir, _period, _L):

        day = datetime.today()
        if self.today == day.strftime('%d'):
            return

        self.today = day.strftime('%d')

        tday = (day - timedelta(int(_period))).strftime('%Y%m%d')

        path = os.path.join(_t_dir, tday)

        try:
            shutil.rmtree(path)
            _L.log(1, 'INF| delete [{0}]'.format(path))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            _L.log(0,
                "ERR| {0}".format(traceback.format_exception(exc_value,
                                                             exc_value,
                                                             exc_traceback)))
        else:
            _L.log(0, 'INF| delete successful [{0}]'.format(path))

        return




