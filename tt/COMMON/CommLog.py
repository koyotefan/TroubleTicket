import time
#import sys
import os
import threading
#import traceback

'''
self.L.log(0, 'ERR| BkmsTelnet::verify_login except:%s' \
                    % repr(traceback.format_exception( exc_value, exc_value,
                            exc_traceback)))

'''

class Log(object):
    def __init__(self, level):
        self.lock = threading.Lock()
        self.level = level

    def init(self, _base_dir, _fname='', _lock = True):
        self.base_dir = _base_dir
        self.fname    = _fname
        self.islock   = _lock

        self.date_    = ''
        self.t_fname_ = ''

    def log(self, level, contents):

        if level > self.level:
            return

        try:
            if self.islock:
                self.lock.acquire()

            sTime = time.strftime('%Y%m%d %m-%d %H:%M:%S')
            self.__get_file_name__(sTime[:8])
            file = open(self.t_fname_, "a+")
            file.write(sTime[9:] + ' %s\n' % contents)
            #file.write(time.strftime('%m-%d %H:%M:%S') + ' %s\n' % contents)
        except :
        #    exc_type, exc_value, exc_traceback = sys.exc_info()
        #    print repr(traceback.format_exception( exc_value, exc_value,
        #                    exc_traceback))
            pass
        finally:
            if file: file.close()

            if self.islock:
                self.lock.release()

    def __get_file_name__(self, _sTime):

        if self.date_ != _sTime:
            t_dir = os.path.join(self.base_dir, _sTime)

            if not os.path.exists(t_dir):
                os.mkdir(t_dir)
            elif os.path.isfile(t_dir):
                os.unlink(t_dir)
                os.mkdir(t_dir)
            else:
                pass

            self.t_fname_ = os.path.join(t_dir, self.fname)
            self.date_ = _sTime
