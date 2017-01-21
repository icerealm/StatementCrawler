import time
'''
Created on Jun 8, 2016

@author: macbookpro
'''

class SleepCounter(object):
    '''
    classdocs
    '''

    def __init__(self, sleep_time_in_sec, max_count_number):
        '''
        Constructor
        '''
        self._sleep_time_in_sec = sleep_time_in_sec
        self._max_count_number = max_count_number
        self._sleep_time_counter = max_count_number
        
    def reset_counter(self):
        self._sleep_time_counter = self._max_count_number
        
    def sleep_when_counter_is_due(self):
        self._sleep_time_counter = self._sleep_time_counter - 1
        if self._sleep_time_counter <= 0:
            time.sleep(self._sleep_time_in_sec)
            self.reset_counter()
