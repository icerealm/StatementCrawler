'''
Created on May 29, 2016

@author: Jakkrit
'''
import logging
import requests
import re
import time
from bs4 import BeautifulSoup
from statementcrawler.provider.data_provider import ThaiStockExchangeMetaData
from statementcrawler.provider.data_provider import CompanyProfilePage
from statementcrawler.provider.data_provider import FileStatement
from statementcrawler.provider.data_provider import headers
from statementcrawler.helper.counter import SleepCounter



class CompanySiteMetaData:
    def __init__(self, company_url, symbol):
        self.company_url = company_url
        self._symbol = symbol
    
    def __str__(self):
        return self._symbol + ':' + self.company_url
    
    def __repr__(self):
        return self._symbol + ':' + self.company_url
    
class Crawler:
    
    '''
    classdocs
    '''    
    def __init__(self):
        '''
        Constructor
        '''
        self._logger = logging.getLogger(__class__.__name__)
        self._stock_exchange_meta_data = ThaiStockExchangeMetaData()
        self._all_menu_symbol_link = []
        self._all_company_meta_data = []
        self.all_company_profile = []
        self.limit_menu_crawling = None
        self.sleep_policy = 3
        self.max_request_counter = 3
    
    def get_stock_exchange(self):
        return self._stock_exchange_meta_data.exchange_nm
        
    def look_up_base_page(self):
        page = requests.get(self._stock_exchange_meta_data.entry_url, headers=headers)
        if not page.content:
            self._logger.warning("there is no page content from:" + self._stock_exchange_meta_data.entry_url)
            return
        self._all_menu_page_reader = BeautifulSoup(page.content, "html.parser")
        
    def crawling_page(self):
        if not self._all_menu_page_reader:
            self._logger.warn("please call look_up_base_page first")
        self._read_all_menu_symbol_in_page(limit=self.limit_menu_crawling)
        self._read_symbol_url_in_page()
        self._read_company_profile() 
    
    def download_all_financial_statements(self):
        if len(self.all_company_profile) > 0:
            for company_profile in self.all_company_profile:
                company_profile.read_statement()
        
    def _read_all_menu_symbol_in_page(self, limit = None):
        self._logger.info("read all menu _symbol...")
        if not limit:
            limit = len(self._stock_exchange_meta_data.all_label_symbol)
        for i in range(limit):
            label_menu_symbol = self._stock_exchange_meta_data.all_label_symbol[i]
            link = self._all_menu_page_reader.find("a", href=True, text=label_menu_symbol)
            self._all_menu_symbol_link.append(self._stock_exchange_meta_data.base_url+link['href'])
        self._logger.info("all menu:" + ';'.join(self._all_menu_symbol_link))
    
    def _read_symbol_url_in_page(self):
        global limit_crawling
        self._logger.info("read _symbol _url...")
        if len(self._all_menu_symbol_link) <= 0:
            self._logger.warning("there is no menu symbols...")
        counter = SleepCounter(self.sleep_policy, self.max_request_counter)
        for menu_url in self._all_menu_symbol_link:
            page = requests.get(menu_url, headers=headers)
            symbols_page_reader = BeautifulSoup(page.content, "html.parser")
            symbol_url_links = symbols_page_reader.find_all('a', href=re.compile('.*companyprofile.do.*'))
            self._parse_to_company_meta_data(menu_url, 
                                             symbol_url_links)
            counter.sleep_when_counter_is_due()
            
            
    
    def _parse_to_company_meta_data(self, menu_url,symbol_url_links):
        if len(symbol_url_links) <= 0:
            self._logger.warning("there is no _symbol _url in page:" + menu_url)
            return
        for symbol_url_link in symbol_url_links:
            symbol_url = self._stock_exchange_meta_data.base_url + symbol_url_link['href']
            company_meta_data = CompanySiteMetaData(company_url= symbol_url, symbol=symbol_url_link.text)
            self._all_company_meta_data.append(company_meta_data)
            self._logger.info("keep company meta data:" + company_meta_data.__str__() + " to memory")
    
    def _read_company_profile(self):
        self._logger.info("read company profile...")
        if len(self._all_company_meta_data) <= 0:
            self._logger.warning("there is no _symbol company meta data")
            return
        counter = SleepCounter(self.sleep_policy, self.max_request_counter)
        for company_meta_data in self._all_company_meta_data:
            self._logger.info("reading company profile, symbol(" + company_meta_data._symbol + ")...")
            company_profile_page = requests.get(company_meta_data.company_url, headers=headers)
            company_profile_reader = BeautifulSoup(company_profile_page.content, "html.parser")
            table = company_profile_reader.find('table', {"class" : "table"})
            rows = table.findAll("div", {"class" : "row"})
            company_nm = rows[0].findAll("div")[1].text
            financial_stmt_link = table.find("a", text=re.compile("Financial Statement.*"))
            company_website = table.find("a", target="_blank", href=re.compile("^((?!\.zip).)*$"))
            company_profile = CompanyProfilePage(company_meta_data.company_url, 
                                                 company_meta_data._symbol,
                                                 company_nm.rstrip(),
                                                 financial_stmt_link["href"] if financial_stmt_link else "",
                                                 financial_stmt_link.text if financial_stmt_link else "",
                                                 company_website["href"] if company_website else "")
            self.all_company_profile.append(company_profile)
            counter.sleep_when_counter_is_due()



class SecondaryCrawler(object):  
              
    def __init__(self):
        self._stock_exchange_meta_data = ThaiStockExchangeMetaData()
        self._logger = logging.getLogger(__class__.__name__)
        self._base_url = self._stock_exchange_meta_data.base_url
        self._entry_url = self._stock_exchange_meta_data.secondary_entry_url
        self._all_file_statements = []
        self._sleep_time = 1
    
    def set_symbols(self, symbols):
        self._symbols = symbols
        
    def get_stock_exchange(self):
        return self._stock_exchange_meta_data.exchange_nm
    
    def get_all_file_statements(self):
        return self._all_file_statements
     
    def _get_crawling_url(self, symbol, page_idx):
        return self._entry_url.replace("<SYMBOL>", symbol).replace("<PAGE>", str(page_idx))

    def _keep_zip_links(self, symbol, zip_links):
        for zip_link in zip_links:
            column = zip_link.parent.parent
            label = column.find("td", text=re.compile("Financial Statement.*"))
            if not label:
                self._logger.warning("there is no label for " + symbol + ", url:" + zip_link["href"])
                continue
            download_url = self._base_url + zip_link["href"]
            download_page = requests.get(download_url, headers=headers)
            download_page_reader = BeautifulSoup(download_page.content, "html.parser")
            zip_url = download_page_reader.find("a", text="Download")
            if not zip_url:
                self._logger.warning("there is no zip url for " + symbol + ", url:" + download_url)
                continue
            file_statement = FileStatement(symbol, zip_url["href"].rstrip(), label.text)
            self._all_file_statements.append(file_statement)

    def _crawling_detail(self, symbol, target_url, current_page_idx=0):
        time.sleep(self._sleep_time)
        self._logger.info("crawling archived file from url:" + target_url)
        company_archived_page = requests.get(target_url, headers=headers)
        if not company_archived_page:
            self._logger.warning("no page found with url:" + target_url)
            return
        company_archived_reader = BeautifulSoup(company_archived_page.content, "html.parser")
        zip_links = company_archived_reader.find_all("a", text="ZIP")
        self._keep_zip_links(symbol, zip_links)
        all_page_nums = company_archived_reader.find("ol", {"class":"nav-number"})
        if not all_page_nums:
            self._logger.warning("no page section in this url:" + target_url)
            return
        links = all_page_nums.find_all("a")
        last_page_str = links[-1].text
        if not last_page_str:
            return
        else:
            last_page_idx = int(last_page_str) - 1
            if current_page_idx < last_page_idx:
                next_page_idx = current_page_idx + 1
                next_target_url = self._get_crawling_url(symbol, next_page_idx)
                self._crawling_detail(symbol, next_target_url, next_page_idx)
    
    def crawling_page(self):
        counter = SleepCounter(self._sleep_time, 1)
        for symbol in sorted(self._symbols):
            company_entry_url = self._get_crawling_url(symbol, 0)
            self._crawling_detail(symbol, company_entry_url)
            counter.sleep_when_counter_is_due()
    