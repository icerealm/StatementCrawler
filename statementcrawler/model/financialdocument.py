from statementcrawler.persistent.financialdocumentdao import FinancialDocumentDao
from statementcrawler.helper.counter import SleepCounter

import os
import logging
import datetime
import calendar
import re

class FileCollector(object):
    
    def __init__(self, exchange, file_statements):
        self._logger = logging.getLogger(__class__.__name__)
        self._exchange = exchange
        self._file_statements = file_statements
    
    def save(self):
        counter = SleepCounter(sleep_time_in_sec = 1, max_count_number = 1)
        for file_statement in self._file_statements:
            try:
                file_statement.download()
            except ConnectionError:
                self._logger.error("Connection error while downloading " + file_statement.get_symbol())
                continue 
            financial_stmt = FinancialStatementDocument(self._exchange, file_statement)
            financial_stmt.save()
            counter.sleep_when_counter_is_due()

class FinancialStatementDocument:
    
    def __init__(self, exchange, financial_stmt):
        self._logger = logging.getLogger(__class__.__name__)
        self._exchange = exchange
        self._symbol = financial_stmt.get_symbol()
        self._financial_stmt = financial_stmt
        self._period_label = financial_stmt.financial_statement_duration()
        self._extension = ""
    
    def convert_period_label_to_datetime(self, period_label):
        is_year = re.match(".*yearly.*", period_label, re.M|re.I)
        is_quarter = re.match(".*(Quarter|quarter|Q\d).*", period_label)
        if is_year and not is_quarter:
            match = re.search('\d{4}', period_label, re.M|re.I)
            year = match.group(0)
            fiscal = datetime.datetime(int(year), 12, 31, 0, 0, 0, 0)
            return fiscal
        if not is_year and is_quarter:
            quarterly = re.sub("[a-zA-Z\s\(\)]", "", period_label)
            if quarterly:
                qtime_array = quarterly.split("/")
                if len(qtime_array) <= 0 or int(qtime_array[0]) > 4:
                    return None
                if len(qtime_array) == 1 and qtime_array[0] < 4:
                    now = datetime.datetime.now()
                    year = now.year
                
                if len(qtime_array[1]) > 4:
                    return None
                if len(qtime_array[1]) < 4:
                    year = int(qtime_array[1]) + 2000
                else:
                    year = int(qtime_array[1])
                month = int(qtime_array[0]) * 3 #convert quarter to month
                last_day_of_month = calendar.monthrange(year, month)[1]
                fiscal = datetime.datetime(year, month, last_day_of_month, 0, 0, 0, 0)
            return fiscal
        return None
    
    def _read_file_from_financial_stmt(self):
        if not self._financial_stmt.file_location():
            self._logger.info("there is no file path for symbol(" + self._symbol + ")")
            return
        _, file_extension = os.path.splitext(self._financial_stmt.file_location())
        self._extension = file_extension
        with open(self._financial_stmt.file_location(), 'rb') as statement_reader:
            buff = statement_reader.read()
            data = bytearray(buff)
        return data
    
    def save(self):
        data = self._read_file_from_financial_stmt()
        if not data:
            self._logger.info("there is no rawfile data for symbol(" + self._symbol + ")")
            return
        self._logger.info("inserting statement file data for symbol(" + self._symbol + ")")
        dao = FinancialDocumentDao()
        fiscal = self.convert_period_label_to_datetime(self._period_label)
        if fiscal:
            dao.insert(exchange = self._exchange, 
                       symbol = self._symbol, 
                       period_label= self._period_label,
                       rawfile = data, 
                       file_extension = self._extension,
                       fiscal=fiscal)
        else:
            self._logger.warning("symbol:%s, there is no fiscal for period_label:[%s])" %(self._symbol, self._period_label))
        