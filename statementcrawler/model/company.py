from statementcrawler.persistent.companydao import CompanyDao
from statementcrawler.model.financialdocument import FinancialStatementDocument
from statementcrawler.helper.counter import SleepCounter
import logging
import re

class CompanyCollector:
    
    def __init__(self, exchange, company_profiles):
        self._exchange = exchange
        self._company_profiles = company_profiles
        
    @staticmethod
    def get_all_symbols(exchange):
        dao = CompanyDao()
        symbols = []
        rows = dao.find_all_symbols()
        for row in rows:
            if exchange == row.exchange:
                symbols.append(row.symbol)
        return symbols
    
    @staticmethod
    def get_all_symbols_with_prefix(exchange, prefix):
        pattern = "^(" + prefix.capitalize() + "|" +prefix.lower() + ").*"
        dao = CompanyDao()
        symbols = []
        rows = dao.find_all_symbols()
        for row in rows:
            if exchange == row.exchange and re.match(pattern, row.symbol):
                symbols.append(row.symbol)
        return symbols
                

    def save(self):
        counter = SleepCounter(sleep_time_in_sec = 2, max_count_number = 2)
        for company_profile in self._company_profiles:
            company_profile.read_statement()
            info = company_profile.company_info()
            company = Company(self._exchange, 
                              info["symbol"],
                              info["company_nm"],
                              info["company_website"]
                              )
            company.save()
            if not company_profile.get_file_statement():
                continue
            financialStatement = FinancialStatementDocument(self._exchange, 
                                                                company_profile.get_file_statement())
            financialStatement.save()
            counter.sleep_when_counter_is_due()
            

        
class Company:
    
    def __init__(self, exchange, symbol, company_name, 
                 company_website=None,business_description=None):
        self._logger = logging.getLogger(__class__.__name__)
        self._exchange = exchange
        self._symbol = symbol
        self._company_name = company_name
        self._company_website = company_website
        self._business_description = business_description
    
        
    def save(self):
        self._logger.info("inserting company data symbol(" + self._symbol + ")...")
        dao = CompanyDao()
        dao.insert(self._exchange, 
                   self._symbol, 
                   self._company_name, 
                   self._company_website, 
                   self._business_description)

        