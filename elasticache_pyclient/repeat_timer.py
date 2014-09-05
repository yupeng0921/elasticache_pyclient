#! /usr/bin/env python

from threading import Thread, Event
from elasticache_pyclient import elasticache_logger

class RepeatTimer(Thread):
    def __init__(self, name, interval, func, args=[], kwargs={}, daemonic=True, break_on_err=True):
        self.interval = interval
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.break_on_err = break_on_err
        self.stop_event = Event()
        Thread.__init__(self,name=name)
        self.setDaemon(daemonic)
    def run(self):
        while not self.stop_event.wait(self.interval):
            # before python2.7, Event.wait() always return None
            # so check whether the event is set
            if self.stop_event.is_set():
                break
            try:
                self.func(*self.args, **self.kwargs)
            except Exception, e:
                if hasattr(self.func, '__name__'):
                    func_name = self.func.__name__
                else:
                    func_name = ''
                msg = '%s %s failed' % (str(self.func), func_name)
                elasticache_logger.exception(msg)
                if break_on_err:
                    break
    def stop_timer(self):
        self.stop_event.set()
