import datetime

DRIVER_PATH = "/opt/airflow/driver/chromedriver"

today = datetime.datetime.now()
day = today.day
month = today.month
year = today.year
ROOT_URL = f"https://www.bblam.co.th/en/products/mutual-funds/historical-daily-navs?p_code=B-TREASURY&date_from=13&month_from=02&year_from=2016&date_to={day}&month_to={month}&year_to={year}"
