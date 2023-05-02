from abc import ABC

from selenium_web_crawler.base_crawing import SeleniumCrawler
from selenium.webdriver.common.by import By
from selenium import webdriver
import pandas as pd


class BualuangCrawlerBase(SeleniumCrawler):
    def __init__(self, driver, name: str, root_url: str) -> None:
        super().__init__(driver, name, root_url)

    def start(self) -> None:
        # Goto Root Url
        self.get_page(self.root_url)

        # Close Popup info
        div = self.loading_wait(self.driver.find_element, (By.CLASS_NAME, "btn-accept"))
        div.click()

    def get_fund_selector(self):
        selector = None
        for i in self.loading_wait(self.driver.find_elements, (By.CLASS_NAME, "cs-placeholder")):
            if i.text == "B-TREASURY : Bualuang Treasury Fund":
                selector = i
        return selector


    def close(self):
        self.driver.close()

    def get_fund_menu(self):
        fund_selector = None
        for i in self.loading_wait(self.driver.find_elements, (By.TAG_NAME, "ul")):
            if "B-TREASURY" in i.text:
                fund_selector = i
                break
        return fund_selector


class BualuangFundsCrawler(BualuangCrawlerBase):
    def __init__(self, driver, name: str, root_url: str) -> None:
        super().__init__(driver, name, root_url)

    def start(self):
        super().start()
        fund_selector = self.get_fund_selector()
        fund_selector.click()
        fund_menu = self.get_fund_menu()
        fund_goto = [i.text for i in self.loading_wait(fund_menu.find_elements, (By.TAG_NAME, 'li'))]
        return fund_goto


class BualuangTableCrawler(BualuangCrawlerBase):
    def __init__(self, driver, name: str, root_url: str) -> None:
        super().__init__(driver, name, root_url)
        self.table = None

    def start(self):
        super().start()

        # Click fund in `Funds selector`
        selector = self.get_fund_selector()
        selector.click()
        self.select_fund()

        # Crawling table
        self.crawing_table()

    def select_fund(self):
        fund_selector = self.get_fund_menu()
        self.click_fund(fund_selector)
        self.click_submit_fund()

    def click_fund(self, fund_selector: object) -> object:
        fund_target = None
        for i in self.loading_wait(fund_selector.find_elements, (By.TAG_NAME, 'li')):
            if self.name in i.text:
                fund_target = i
                break
        fund_target.click()

    def click_submit_fund(self):
        self.loading_wait(self.driver.find_elements, (By.CLASS_NAME, 'submit'))[1].click()

    def crawing_table(self):
        script = """
        var rows = document.querySelectorAll("table tr");
        var filteredRows = [];
        for (var i = 1; i < rows.length; i++) {
            var cells = rows[i].querySelectorAll("td");
            if (cells.length === 6 && cells[1].textContent.trim() !== "") {
                var row = [];
                for (var j = 0; j < cells.length; j++) {
                    row.push(cells[j].textContent.trim());
                }
                filteredRows.push(row);
            }
        }
        return filteredRows;
        """
        table_rows = self.driver.execute_script(script)

        # To dataframe
        schemas = ['date', 'NAV', 'NavperUnit', 'sellingPrice', 'redemptionPrice', 'change']
        df = pd.DataFrame(table_rows, columns=schemas)

        # To float
        df.replace('N/A', None, inplace=True)
        numerical = ['NAV', 'NavperUnit', 'sellingPrice', 'redemptionPrice', 'change']
        for i in numerical:
            df[i] = df[i].str.replace(',', "")
            df[i] = df[i].astype(float)
        df['fund_name'] = self.name
        self.table = df.copy()

    def close(self):
        self.driver.close()
