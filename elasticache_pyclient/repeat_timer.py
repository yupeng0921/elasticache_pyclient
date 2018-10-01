# -*- coding: utf-8 -*-

from threading import Thread, Event
import logging


logger = logging.getLogger('elasticache')


class RepeatTimer(Thread):

    def __init__(
            self, name, interval, func, args=[], kwargs={},
            daemonic=True, break_on_err=False):
        self.interval = interval
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.break_on_err = break_on_err
        self.stop_event = Event()
        super(RepeatTimer, self).__init__(name=name)
        self.setDaemon(daemonic)

    def run(self):
        while not self.stop_event.wait(self.interval):
            # before python2.7, Event.wait() always return None
            # so check whether the event is set
            if self.stop_event.is_set():
                break
            try:
                self.func(*self.args, **self.kwargs)
            except Exception as e:
                if hasattr(self.func, '__name__'):
                    func_name = self.func.__name__
                else:
                    func_name = ''
                msg = '%s %s failed' % (str(self.func), func_name)
                logger.warning(msg, exc_info=True)
                if self.break_on_err:
                    break

    def stop_timer(self):
        self.stop_event.set()
