from statementcrawler.helper.database import get_db_session

'''
Created on Jun 6, 2016

@author: macbookpro
'''
import logging

class FinancialDocumentDao(object):
    
        def __init__(self):
            '''
            Constructor
            '''
            self._logger = logging.getLogger(__class__.__name__)

        
        def insert(self, exchange, symbol, period_label, rawfile, file_extension, fiscal):
            session = get_db_session()
            param = {"exchange": exchange, 
                     "symbol": symbol, 
                     "period_label": period_label, 
                     "rawfile": rawfile,
                     "file_extension": file_extension,
                     "fiscal_date": fiscal}
            self._logger.info("insert data for symbol:%s with period label:%s" %(symbol, period_label))
            session.execute(
                """
                INSERT INTO FinancialStatementDocument(exchange, symbol, period_label, rawfile, file_extension, fiscal_date)
                VALUES (%(exchange)s, %(symbol)s, %(period_label)s, %(rawfile)s, %(file_extension)s, %(fiscal_date)s)
                """
                ,param
            )