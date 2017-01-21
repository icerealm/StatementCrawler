import re
import tempfile
import logging
import requests
import json

headers = {
    'User-Agent': 'MonkeySpider/0.1',
    'From': 'local_alpha_test.org'  # This is another valid field
}

class ThaiStockExchangeMetaData:
    
    def __init__(self):
        self.exchange_nm = "SET"
        self.base_url = "http://www.set.or.th"
        self.entry_url = "http://www.set.or.th/set/commonslookup.do?language=en&country=US"
        self.all_label_symbol = ('0-9', 'A', 'B', 'C', 'D', 'E', 'F', 
                                 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
                                 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
                                 'W', 'X', 'Y', 'Z')
        self.sub_entry_url = "http://www.set.or.th/set/commonslookup.do?language=en&country=US&prefix=<PREFIX>"
        self.company_url = "http://www.set.or.th/set/companyprofile.do?symbol=<SYMBOL>&ssoPageId=4&language=en&country=US"
        self.secondary_entry_url = "http://www.set.or.th/set/companynews.do?symbol=<SYMBOL>&currentpage=<PAGE>&language=en&ssoPageId=8&country=US"
        


class CompanyProfilePage:
    
    def __init__(self, url, symbol, company_nm, file_statement_url="", financial_stmt_label="", company_website=""):
        self._url = url
        self._symbol = symbol
        self._company_nm = company_nm
        self._file_statement = FileStatement(symbol, file_statement_url, financial_stmt_label)
        self._company_website = company_website
    
    def company_info(self):
        return {"url": self._url,
                "symbol": self._symbol,
                "company_nm": self._company_nm,
                "file_statement": self._file_statement.__str__(),
                "company_website": self._company_website}
    
    def get_file_statement(self):
        return self._file_statement
    
    def __str__(self):
        data = self.company_info()
        return json.dumps(data)
    
    def __repr__(self):
        data = self.company_info()
        return json.dumps(data)   
         
    def read_statement(self):
        self._file_statement.download()
    
class FileStatement:
    
    def __init__(self, symbol, url, file_label_nm):
        self._logger = logging.getLogger(__class__.__name__)
        self._url = url
        self._symbol = symbol
        self._file_label_nm = file_label_nm
        self._local_resource = ""
        self._duration = ""
        
    def file_info(self):
        return {"url": self._url, 
                "symbol": self._symbol, 
                "file_label_nm": self._file_label_nm, 
                "local_resource": self._local_resource,
                "duration": self._duration}
        
    def __str__(self):
        data = self.file_info()
        return json.dumps(data)
    
    def __repr__(self):
        data = self.file_info()
        return json.dumps(data)
    
    def get_symbol(self):
        return self._symbol
    
    def file_location(self):
        return self._local_resource
    
    def financial_statement_duration(self):
        return self._file_label_nm
    
    def get_duration_time_in_file(self):
        if not self._file_label_nm:
            self._logger.info("this stock(" + self._symbol + ") does not have Finanacial Statement in the page")
            return
        search_txt = re.search("Financial Statement (.+?)$", self._file_label_nm).group(1)
        self._duration = search_txt
        
    def download(self):
        if not self._url:
            self._logger.info("there is no url from this stock(" + self._symbol + ")  to download")
            return
        local_filename = self._symbol+"_"+self._url.split('/')[-1]
        path_to_file = tempfile.gettempdir() + "/" + local_filename
        self._logger.info("downloading with _url:" + self._url + " and save to dir:" + path_to_file)
        r = requests.get(self._url)       
        with open(path_to_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        self._local_resource = path_to_file
