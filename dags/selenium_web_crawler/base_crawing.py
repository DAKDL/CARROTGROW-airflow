from abc import ABC, abstractmethod
from typing import Callable, List


class SeleniumCrawler(ABC):

    def __init__(self, driver, name: str, root_url: str) -> None:
        self.name = name
        self.root_url = root_url
        self.driver = driver

    @staticmethod
    def loading_wait(find_func: Callable, arg: tuple) -> List[object]:
        out = None
        while True:
            out = find_func(*arg)
            if out is not None:
                break
        return out

    def get_page(self, page_url: str) -> None:
        self.driver.get(page_url)

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def status(self):
        status = 'inactive'
        if self.driver != None:
            status = "active"
        return status

    def __str__(self):
        status = self.status()
        return f"<SeleniumCrawler {self.name} status: {status}>"