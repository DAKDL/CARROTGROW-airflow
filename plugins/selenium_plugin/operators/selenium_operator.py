from typing import Callable

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class SeleniumOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            task_id: str,
            process_browser: Callable,
            remote_url: str = "http://chrome:4444/wd/hub",
            browser_capabilities=None,
            process_args=(),
            *args,
            **kwargs,
    ):
        super().__init__(task_id=task_id, *args, **kwargs)
        if browser_capabilities is None:
            browser_capabilities = browser_capabilities = {
                "browserName": "chrome",
                "browserVersion": "112.0",
                "platformName": "LINUX",
                "se:noVncPort": 7900,
                "se:vncEnabled": True
            }
        self.process_args = process_args
        self.process_browser = process_browser
        self.remote_url = remote_url
        self.browser_capabilities = browser_capabilities

    def execute(self, context):
        self.log.info("Initializing Selenium WebDriver")
        driver = webdriver.Remote(
            command_executor=self.remote_url, desired_capabilities=self.browser_capabilities
        )
        self.log.info("Connect to Chrome successful")
        driver.set_window_size(1920, 1080)
        self.log.info("Set window size to 1920 x 1080")
        print(driver)
        try:
            self.log.info("Processing browser with provided function")
            results = self.process_browser(driver, *self.process_args)
            return results
        finally:
            self.log.info("Quitting WebDriver")
            driver.quit()
