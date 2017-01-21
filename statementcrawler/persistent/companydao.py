from statementcrawler.helper.database import get_db_session
'''
Created on May 29, 2016

@author: macbookpro
'''

class CompanyDao(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        
    def insert(self, exchange, symbol, company_nm, company_website = "", description = ""):
        session = get_db_session()
        param = {"exchange": exchange, 
                 "symbol": symbol, 
                 "company_name": company_nm, 
                 "company_website": company_website,
                 "business_description": description}
        session.execute(
            """
            INSERT INTO Company(exchange, symbol, company_name, company_website, business_description)
            VALUES (%(exchange)s, %(symbol)s, %(company_name)s, %(company_website)s, %(business_description)s)
            """
            ,param
        )
    
    def find_all_symbols(self):
        session = get_db_session()
        rows = session.execute("SELECT exchange, symbol from company")
        return rows
