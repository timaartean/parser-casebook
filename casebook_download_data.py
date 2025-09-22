import os
import re
import sys
import time
import random
import logging
import glob
from datetime import datetime, timedelta
from typing import Literal
from dotenv import load_dotenv
from urllib.parse import urlparse
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

load_dotenv()

# Настройка логирования на русском
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('casebook_download.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('CasebookDownload')


def log_step(step_description):
    """Декоратор для логирования начала и окончания шага"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.info(f"🟢 Начало: {step_description}")
            try:
                result = func(*args, **kwargs)
                logger.info(f"✅ Успешно: {step_description}")
                return result
            except Exception as e:
                logger.error(f"❌ Ошибка при выполнении '{step_description}': {str(e)}")
                raise

        return wrapper

    return decorator


class CasebookDownloader:
    def __init__(self, court_type=None, category_code=None, min_summ=None):
        # Каталог загрузки можно переопределить через DOWNLOAD_DIR
        env_download_dir = (os.getenv('DOWNLOAD_DIR') or '').strip()
        self.abs_path = os.path.abspath(env_download_dir) if env_download_dir else os.getcwd()
        try:
            os.makedirs(self.abs_path, exist_ok=True)
        except Exception:
            pass
        self.court_type = court_type
        self.category_code = category_code
        self.min_summ = min_summ
        self.today = datetime.now().strftime('%d.%m.%Y')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
        self.driver = None
        self.wait = None
        # Установка диапазона дат из ENV с дефолтом на «сегодня»
        env_date = os.getenv('CASEBOOK_DATE')
        env_date_from = os.getenv('CASEBOOK_DATE_FROM')
        env_date_to = os.getenv('CASEBOOK_DATE_TO')

        if env_date and env_date.strip():
            self.date_from_str = env_date.strip()
            self.date_to_str = env_date.strip()
        else:
            from_val = env_date_from.strip() if env_date_from and env_date_from.strip() else None
            to_val = env_date_to.strip() if env_date_to and env_date_to.strip() else None
            if from_val or to_val:
                self.date_from_str = from_val or to_val
                self.date_to_str = to_val or from_val
            else:
                self.date_from_str = self.today
                self.date_to_str = self.today

    def initialize(self):
        """Инициализация драйвера и настроек"""
        try:
            logger.info("Инициализация процесса загрузки")

            # Удаление старого файла, если существует
            if os.path.exists(os.path.join(self.abs_path, 'ArbitrageSearchExport.csv')):
                os.remove(os.path.join(self.abs_path, 'ArbitrageSearchExport.csv'))

            options = Options()
            options.page_load_strategy = 'eager'
            prefs = {
                "download.default_directory": self.abs_path,
                "download.prompt_for_download": False,
                "safebrowsing.enabled": True
            }
            options.add_experimental_option("prefs", prefs)
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-gpu")

            # Управление режимом headless через переменную окружения HEADLESS (true/false)
            headless_env = (os.getenv('HEADLESS') or 'true').strip().lower()
            run_headless = headless_env in ('1', 'true', 'yes', 'y')
            try:
                self.driver = uc.Chrome(headless=run_headless, options=options)
            except Exception as start_err:
                # Авто-подбор major-версии Chrome при несовпадении драйвера
                msg = str(start_err)
                m = re.search(r"Current browser version is (\d+)", msg)
                if m:
                    major = int(m.group(1))
                    self.driver = uc.Chrome(headless=run_headless, options=options, version_main=major)
                else:
                    raise

            self.driver.set_page_load_timeout(120)
            time.sleep(1)
            try:
                self.driver.set_window_size(1400, 1000)
            except Exception:
                pass
            self.driver.delete_all_cookies()
            self.wait = WebDriverWait(self.driver, 60)
            # Явно разрешаем скачивания и задаём каталог через CDP (надёжно для headless/macOS)
            try:
                self.driver.execute_cdp_cmd('Page.setDownloadBehavior', {
                    'behavior': 'allow',
                    'downloadPath': self.abs_path
                })
                logger.info(f"Каталог загрузки: {self.abs_path}")
            except Exception as cdp_err:
                logger.warning(f"Не удалось применить Page.setDownloadBehavior: {cdp_err}")

        except Exception as e:
            if hasattr(self, 'driver') and self.driver:
                self.driver.quit()
            logger.error(f"Ошибка инициализации: {str(e)}")
            raise

    @log_step("Авторизация в Casebook")
    def login(self):
        """Выполнение авторизации в системе"""
        self.driver.get(os.getenv('CASEBOOK_LOGIN_URL'))

        # Ввод логина
        self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[@name='UserName']")))
        login_field = self.driver.find_element(By.XPATH, "//input[@name='UserName']")
        login_field.send_keys(os.getenv('CASEBOOK_LOGIN'))

        # Ввод пароля
        self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[@name='Password']")))
        password_field = self.driver.find_element(By.XPATH, "//input[@name='Password']")
        password_field.send_keys(os.getenv('CASEBOOK_PASSWORD'))

        old_url = self.driver.current_url

        # Нажатие кнопки входа
        self.driver.find_element(
            By.XPATH,
            "//div[@class='b-form-control']/div[contains(@class, 'ui-button')]"
        ).click()
        # Ждём, когда произойдёт переход на другую страницу (смена URL)
        self.wait.until(EC.url_changes(old_url))

    @log_step("Переход на страницу поиска")
    def go_to_search_page(self):
        """Переход на страницу поиска дел"""
        old_url = self.driver.current_url

        self.wait.until(EC.presence_of_element_located((
            By.XPATH, "//a[@href='/app/request']"
        ))).click()
        # Ждём, когда произойдёт переход на другую страницу (смена URL)
        self.wait.until(EC.url_changes(old_url))

    def go_to_search_page_via_url(self):
        """Переход на страницу расширенного поиска напрямую по ссылке"""
        current = self.driver.current_url
        parsed = urlparse(current)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        target = origin + "/app/request/new/cases"
        self.driver.get(target)
        # Ждём появления ключевого фильтра "Укажите суд"
        self.wait.until(EC.presence_of_element_located((
            By.XPATH, "//div[@data-title='Укажите суд']"
        )))

    @log_step("Настройка параметров поиска")
    def setup_search_parameters(self):
        """Установка параметров поиска дел"""
        # Выбор типа суда
        court_filter = self.wait.until(EC.element_to_be_clickable((
            By.XPATH, "//div[@data-title='Укажите суд']"
        )))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", court_filter)
        self.driver.execute_script("arguments[0].click();", court_filter)
        court_option = self.wait.until(EC.element_to_be_clickable((
            By.XPATH, f"//label[contains(text(), '{self.court_type}')]"
        )))
        self.driver.execute_script("arguments[0].click();", court_option)
        # Выбор категории спора по коду
        category_container = self.wait.until(EC.element_to_be_clickable((
            By.XPATH, "//div[contains(@class,'b-filter-container') and contains(@class,'js-filter-container') and @data-title='Укажите категорию спора']"
        )))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", category_container)
        self.driver.execute_script("arguments[0].click();", category_container)

        # Пытаемся вводить код в поле ввода внутри категории (если есть)
        try:
            input_inside = self.wait.until(EC.presence_of_element_located((
                By.XPATH, "//div[contains(@class,'b-filter--case_categories')]//input"
            )))
            input_inside.clear()
            input_inside.send_keys(str(self.category_code))
            time.sleep(0.5)
        except Exception:
            try:
                actions = ActionChains(self.driver)
                for ch in str(self.category_code):
                    actions.send_keys(ch)
                actions.perform()
                time.sleep(0.5)
            except Exception:
                pass

        # Пытаемся выбрать пункт, содержащий код категории
        label_elem = None
        try:
            label_elem = self.wait.until(EC.presence_of_element_located((
                By.XPATH, f"//div[contains(@class,'b-filter--case_categories')]//li[contains(@class,'b-filter-option')]//label[contains(., '{str(self.category_code)}')]"
            )))
        except Exception:
            pass

        if not label_elem:
            # Фолбэк: берём первый элемент списка
            lis = self.wait.until(EC.presence_of_all_elements_located((
                By.CSS_SELECTOR, "div.b-filter--case_categories ul.b-filter-dropdown-list li.b-filter-option"
            )))
            if lis:
                label_elem = lis[0].find_element(By.TAG_NAME, "label")

        if label_elem:
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", label_elem)
            self.driver.execute_script("arguments[0].click();", label_elem)
        # Установка даты "с"
        # Закрываем дропдаун категорией кликом вне (по телу) и ESC, чтобы не перекрывал поля
        # self.driver.execute_script("document.body.click();")
        # ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(0.5)
        # На некоторых страницах дропдаун категории перекрывает поля дат — закрываем ESC
        try:
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(0.2)
        except Exception:
            pass

        # Устанавливаем даты с повторными попытками и верификацией значения
        self._set_date_field_with_retry("//input[@data-name='from']", self.date_from_str)
        self._set_date_field_with_retry("//input[@data-name='to']", self.date_to_str)

        sum_param = self.driver.find_elements(By.XPATH, "//div[@data-id='param-sum']")
        if not sum_param:
            # Добавление поля для суммы 
            self.driver.find_element(
                By.XPATH, "//div[contains(@class, 'b-operator-button')]"
            ).click()
            self.wait.until(EC.presence_of_element_located((
                By.XPATH, "//div[@data-id='param-sum']"
            ))).click()
            time.sleep(0.2)
        # Указание минимальной суммы
        min_summ_field = self.wait.until(EC.presence_of_element_located((
            By.XPATH, "//input[@name='minSum']"
        )))
        min_summ_field.send_keys(self.min_summ)
        time.sleep(0.2)

    @log_step("Выполнение поиска дел")
    def perform_search(self):
        """Запуск поиска дел"""
        self.driver.find_element(
            By.XPATH, "//div[contains(@class, 'b-quick_menu-button--search')]"
        ).click()
        # Ожидаем, когда появится блок с количеством результатов
        self.wait.until(EC.presence_of_element_located((By.ID, "search_results_total")))

    def _set_date_field_with_retry(self, xpath: str, value: str):
        """Надёжно установить дату в поле: клик, очистка, ввод, проверка и JS-фолбэк."""
        elem = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elem)
        except Exception:
            pass
        try:
            self.driver.execute_script("arguments[0].click();", elem)
        except Exception:
            try:
                elem.click()
            except Exception:
                pass
        # Очистка: clear + Cmd/Ctrl+A + Delete
        try:
            elem.clear()
        except Exception:
            pass
        try:
            modifier = Keys.COMMAND if sys.platform == 'darwin' else Keys.CONTROL
            elem.send_keys(modifier, 'a')
            elem.send_keys(Keys.DELETE)
        except Exception:
            pass
        time.sleep(0.1)
        try:
            elem.send_keys(value)
        except Exception:
            pass
        time.sleep(0.2)
        actual = (elem.get_attribute('value') or '').strip()
        if actual != value:
            try:
                self.driver.execute_script(
                    "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input', {bubbles:true})); arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                    elem, value
                )
                time.sleep(0.1)
            except Exception:
                pass
            actual = (elem.get_attribute('value') or '').strip()

    @log_step("Проверка количества результатов")
    def get_results_count(self):
        """Считать число найденных дел из блока с id=search_results_total"""
        try:
            elem = self.wait.until(EC.presence_of_element_located((By.ID, "search_results_total")))
            text = (elem.text or "").replace('\xa0', ' ').strip()

            # Универсальный парсинг: берём число после "Найдено", регистронезависимо; запасной парсер — первое число в строке
            match = re.search(r"найдено\s+([\d\s]+)", text, re.IGNORECASE)
            if not match:
                match = re.search(r"([\d\s]+)", text)
            if match:
                return int(re.sub(r"\s+", "", match.group(1)))
            return 0
        except Exception:
            return 0

    @log_step("Скачивание результатов")
    def download_results(self):
        """Скачивание результатов в CSV формате"""
        # Открытие меню
        self.driver.find_element(
            By.XPATH, "(//div[contains(@class, 'js-extra_menu')])[1]"
        ).click()

        # Выбор опции экспорта
        start_ts = time.time()
        self.wait.until(EC.presence_of_element_located((
            By.XPATH, "//div[@id='extra_menu_subpartition']/li[2]"
        ))).click()

        # Ожидание загрузки файла: учитываем .crdownload и варианты имён
        target_name = os.path.join(self.abs_path, 'ArbitrageSearchExport.csv')
        spent_time = 0
        downloaded_path: str | None = None
        while True:
            if spent_time >= 300:  # 5 минут timeout
                raise TimeoutError("Превышено время ожидания загрузки файла")

            if os.path.exists(target_name):
                downloaded_path = target_name
                break

            candidates = [
                p for p in glob.glob(os.path.join(self.abs_path, 'ArbitrageSearchExport*.csv'))
                if not p.endswith('.crdownload') and os.path.getmtime(p) >= start_ts - 2
            ]
            cr_in_progress = any(
                os.path.getmtime(p) >= start_ts - 2
                for p in glob.glob(os.path.join(self.abs_path, '*.crdownload'))
            )
            if candidates and not cr_in_progress:
                downloaded_path = max(candidates, key=os.path.getmtime)
                break

            time.sleep(1)
            spent_time += 1
            if spent_time % 10 == 0:
                logger.info(f"Ожидание загрузки файла... {spent_time} сек.")

        if downloaded_path and os.path.abspath(downloaded_path) != os.path.abspath(target_name):
            try:
                if os.path.exists(target_name):
                    os.remove(target_name)
                os.replace(downloaded_path, target_name)
            except Exception as rn_err:
                logger.warning(f"Не удалось переименовать {downloaded_path} → ArbitrageSearchExport.csv: {rn_err}")

    def execute(self):
        """Основной метод выполнения процесса"""
        try:
            self.initialize()
            self.login()
            self.go_to_search_page()
            self.setup_search_parameters()
            self.perform_search()
            results_count = self.get_results_count()
            # Преобразуем results_count в строку с эмодзи-цифрами для логов

            emoji_results_count = num_to_emoji(results_count)
            cases_word = pluralize_cases(results_count)

            logger.info(f"Результаты поиска: {emoji_results_count} {cases_word}")

            if results_count > 0:
                self.download_results()
                return True, f"Файл успешно загружен. Найдено: {emoji_results_count} {cases_word}"
            else:
                return False, "Найдено 0️⃣ арбитражных дел"

        except Exception as e:
            logger.error(f"Критическая ошибка: {str(e)}")
            return False, f"Ошибка: {str(e)}"
        finally:
            if hasattr(self, 'driver') and self.driver:
                try:
                    self.driver.quit()
                    logger.info("Браузер закрыт")
                except Exception as quit_error:
                    logger.error(f"Ошибка при закрытии браузера: {str(quit_error)}")

    def setup_search_parameters_for(self, court_type, category_code, min_summ):
        """Обёртка для настройки параметров под конкретный запрос"""
        self.court_type = court_type
        self.category_code = category_code
        self.min_summ = min_summ
        self.setup_search_parameters()


def run_casebook_driver(court_type, category_code, min_summ):
    """Одноразовый запуск (с обратной совместимостью по интерфейсу)"""
    downloader = CasebookDownloader(court_type, category_code, min_summ)
    return downloader.execute()


def create_casebook_session() -> CasebookDownloader:
    """Создать и залогинить сессию Casebook для множества запросов"""
    downloader = CasebookDownloader()
    downloader.initialize()
    downloader.login()
    return downloader


def process_casebook_request(downloader: CasebookDownloader, court_type: str, category_code: str, min_summ: str, date_from: str | None = None):
    """Обработать один запрос в рамках открытой сессии. Возвращает (bool, count)."""
    # Настройка дат: приоритет параметра функции, затем ENV, затем «сегодня»
    if date_from and date_from.strip():
        downloader.date_from_str = date_from.strip()
        downloader.date_to_str = date_from.strip()
    else:
        env_date = os.getenv('CASEBOOK_DATE')
        env_date_from = os.getenv('CASEBOOK_DATE_FROM')
        env_date_to = os.getenv('CASEBOOK_DATE_TO')
        if env_date and env_date.strip():
            downloader.date_from_str = env_date.strip()
            downloader.date_to_str = env_date.strip()
        elif (env_date_from and env_date_from.strip()) or (env_date_to and env_date_to.strip()):
            from_val = env_date_from.strip() if env_date_from and env_date_from.strip() else None
            to_val = env_date_to.strip() if env_date_to and env_date_to.strip() else None
            downloader.date_from_str = from_val or to_val or downloader.today
            downloader.date_to_str = to_val or from_val or downloader.today
        else:
            downloader.date_from_str = downloader.today
            downloader.date_to_str = downloader.today

    downloader.go_to_search_page_via_url()
    downloader.setup_search_parameters_for(court_type, category_code, min_summ)
    downloader.perform_search()
    results_count = downloader.get_results_count()
    try:
        logger.info(f"Результаты поиска: {num_to_emoji(results_count)} {pluralize_cases(results_count)}")
    except Exception:
        pass
    if results_count > 0:
        downloader.download_results()
        return True, results_count
    return False, 0


def close_casebook_session(downloader: CasebookDownloader):
    """Закрыть браузер и завершить сессию"""
    if hasattr(downloader, 'driver') and downloader.driver:
        try:
            downloader.driver.quit()
            logger.info("Браузер закрыт")
        except Exception as quit_error:
            logger.error(f"Ошибка при закрытии браузера: {str(quit_error)}")

def num_to_emoji(num):
    emoji_digits = {
        '0': '0️⃣',
        '1': '1️⃣',
        '2': '2️⃣',
        '3': '3️⃣',
        '4': '4️⃣',
        '5': '5️⃣',
        '6': '6️⃣',
        '7': '7️⃣',
        '8': '8️⃣',
        '9': '9️⃣'
    }
    return ''.join(emoji_digits.get(d, d) for d in str(num))

def pluralize_cases(n):
    n = abs(int(n))
    if 11 <= (n % 100) <= 14:
        return "дел"
    last = n % 10
    if last == 1:
        return "дело"
    elif 2 <= last <= 4:
        return "дела"
    else:
        return "дел"


if __name__ == "__main__":
    # Пример использования
    result, message = run_casebook_driver(
        court_type="Арбитражный суд города Москвы",
        category_code="5.1",
        min_summ="1000000"
    )
    logger.info(f"Результат выполнения: {result}, Сообщение: {message}")