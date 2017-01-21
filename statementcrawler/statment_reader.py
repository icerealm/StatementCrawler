import sys
import logging
from statementcrawler.helper.config import init_configuration
from statementcrawler.helper.database import init_db_session, shutdown_all_db_session
from statementcrawler.helper.logger import init_logger
from statementcrawler.crawler.stock_crawler import Crawler
from statementcrawler.crawler.stock_crawler import SecondaryCrawler
from statementcrawler.model.company import CompanyCollector
from statementcrawler.model.financialdocument import FileCollector


def process(crawler_running="", symbol_prifixs=""):
    
    if not crawler_running or crawler_running == 1:
        crawler = Crawler()
    #     crawler.limit_menu_crawling = 2
        crawler.look_up_base_page()
        crawler.crawling_page()
        collector = CompanyCollector(crawler.get_stock_exchange(), crawler.all_company_profile)
        collector.save()
    if not crawler_running or crawler_running == 2:
        if not symbol_prifixs:
            symbols = CompanyCollector.get_all_symbols("SET")
            secondary_crawler = SecondaryCrawler()
            secondary_crawler.set_symbols(symbols)
            secondary_crawler.crawling_page()
            file_collector = FileCollector(secondary_crawler.get_stock_exchange(), secondary_crawler.get_all_file_statements())
            file_collector.save()
        else:
            symbol_prefix_array = symbol_prifixs.split(",")
            for symbol_prefix in symbol_prefix_array:
                try:
                    symbols = CompanyCollector.get_all_symbols_with_prefix("SET", symbol_prefix)
                    secondary_crawler = SecondaryCrawler()
                    secondary_crawler.set_symbols(symbols)
                    secondary_crawler.crawling_page()
                    file_collector = FileCollector(secondary_crawler.get_stock_exchange(), secondary_crawler.get_all_file_statements())
                    file_collector.save()
                except BaseException as e:
                    logging.getLogger().error("error during processing symbol prefix:%s, error msg:%s" %(symbol_prefix, str(e)))


def main(argv=None):
    init_logger()
    logging.getLogger().info("start running StatementReader...")
    
    if argv is None:
        argv = sys.argv
    init_configuration()
    init_db_session()
    process(crawler_running=2, symbol_prifixs="P,S")
    shutdown_all_db_session()
               
        
if __name__ == "__main__":
    sys.exit(main())