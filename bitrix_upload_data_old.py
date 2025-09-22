import re
import os
import time
import random
import logging
import pandas as pd
from dotenv import load_dotenv
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

load_dotenv()

# Настройка логирования на русском
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bitrix_upload.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('BitrixUpload')


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


class BitrixUploader:
    def __init__(self):
        self.abs_path = os.getcwd()
        self.driver = None
        self.wait = None
        self.status = "Умный сценарий не был запущен"

    def initialize(self):
        """Инициализация драйвера и ожиданий"""
        try:
            logger.info("Инициализация процесса загрузки")
            self.driver = uc.Chrome(headless=True)
            self.driver.set_page_load_timeout(120)
            self.wait = WebDriverWait(self.driver, 30)
            time.sleep(2)
        except Exception as e:
            if hasattr(self, 'driver') and self.driver:
                self.driver.quit()
            logger.error(f"Ошибка инициализации: {str(e)}")
            raise

    @log_step("Авторизация в Bitrix24")
    def login(self):
        """Выполнение авторизации"""
        self.driver.get(os.getenv('BITRIX_LOGIN_URL'))

        # Ввод логина
        self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[@id='login']")))
        login_field = self.driver.find_element(By.XPATH, "//input[@id='login']")
        login_field.send_keys(os.getenv('BITRIX_LOGIN'))
        self.driver.find_element(
            By.XPATH,
            "//button[contains(@class, 'b24net-login-enter-form__continue-btn')]"
        ).click()

        # Ввод пароля
        self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='password']")))
        password_field = self.driver.find_element(By.XPATH, "//input[@type='password']")
        password_field.send_keys(os.getenv('BITRIX_PASSWORD'))
        self.driver.find_element(
            By.XPATH,
            "//button[contains(@class, 'b24net-password-enter-form__continue-btn')]"
        ).click()
        time.sleep(random.uniform(2, 4))

    @log_step("Переход на страницу канбана")
    def go_to_kanban(self):
        """Переход на страницу канбана"""
        self.driver.get(os.getenv('BITRIX_KANBAN_PAGE'))
        time.sleep(2)

    @log_step("Загрузка файла")
    def upload_file(self):
        """Загрузка файла в систему"""
        self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='file']")))
        file_input = self.driver.find_element(By.XPATH, "//input[@type='file']")
        path_to_file = os.path.join(self.abs_path, 'CleanedArbitrage.csv')
        file_input.send_keys(path_to_file)
        time.sleep(2)

    @log_step("Настройка параметров импорта")
    def configure_import(self):
        """Настройка параметров импорта"""
        # Нажатие кнопки Далее
        self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[@id='next']"))).click()

        # Выбор кодировки
        self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[@id='popup_window']")))
        cod_xpath = "//div[@id='popup-window-content-popup_window']//button[contains(text(), 'Номер дела')]"
        self.driver.find_element(By.XPATH, cod_xpath).click()
        time.sleep(2)

        # Настройка полей
        fields = {
            'Дата регистрации дела': "//select[@name='IMPORT_FILE_FIELD_1']",
            'Ссылка на Casebook': "//select[@name='IMPORT_FILE_FIELD_3']",
            'Исковые требования в деле': "//select[@name='IMPORT_FILE_FIELD_8']"
        }

        for field_name, xpath in fields.items():
            self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            self.driver.find_element(By.XPATH, xpath).send_keys(field_name)
            time.sleep(1)

        # Дважды нажимаем "Далее" для завершения настройки
        for _ in range(2):
            self.wait.until(EC.presence_of_element_located((
                By.XPATH, "//input[@title='Перейти к следующему шагу']"
            ))).click()
            time.sleep(2)

    @log_step("Получение статистики импорта")
    def get_import_stats(self):
        """Получение статистики по импортированным лидам"""
        try:
            import_leads = self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'crm_import_entity')]"))
            )
            logger.info(f"Результат импорта: {import_leads.text.strip()}")
        except Exception as e:
            logger.warning(f"Не удалось получить статистику импорта: {str(e)}")

    @log_step("Обработка CSV файла")
    def process_csv_file(self):
        """Обработка CSV файла и сохранение номеров дел"""
        try:
            data = pd.read_csv(
                os.path.join(self.abs_path, 'CleanedArbitrage.csv'),
                sep=';', encoding='windows-1251').to_dict(orient='records')

            with open(os.path.join(self.abs_path, 'cases_num.txt'), 'a', encoding='utf-8') as f:
                for line in data:
                    case_num = line['Номер дела']
                    case_for_file = re.sub(r'[^\d]', '', case_num)
                    f.write(f'{case_for_file}\n')
        except Exception as e:
            logger.error(f"Ошибка при обработке файла: {str(e)}")

    @log_step("Настройка фильтров для лидов")
    def setup_filters(self):
        """Настройка фильтров для отбора лидов"""
        try:
            # Открытие фильтра
            self.wait.until(EC.presence_of_element_located((
                By.XPATH, "//input[@class='main-ui-filter-search-filter' and contains(@id, 'CRM_LEAD_LIST_')]"
            ))).click()
            time.sleep(1)

            # Выбор стадии
            self.wait.until(EC.presence_of_element_located((
                By.XPATH, "(//div[@data-name='STATUS_ID'])[2]"
            ))).click()
            time.sleep(1)

            # Выбор "Заявки, вх. звонки"
            self.wait.until(EC.presence_of_element_located((
                By.XPATH, "//div[contains(@data-item, 'ЗАЯВКИ, ВХ. ЗВОНКИ')]"
            ))).click()
            time.sleep(1)

            # Повторный выбор фильтра
            self.wait.until(EC.presence_of_element_located((
                By.XPATH, "(//div[@data-name='STATUS_ID'])[2]"
            ))).click()
            time.sleep(1)

            # Нажатие кнопки "Найти"
            self.wait.until(EC.presence_of_element_located((
                By.XPATH, "//div[@class='main-ui-filter-field-preset-button-container']/div/button[1]"
            ))).click()
            time.sleep(1)
        except Exception as e:
            logger.error(f"Ошибка при настройке фильтров: {str(e)}")
            raise

    @log_step("Выбор всех лидов")
    def select_all_leads(self):
        """Попытка выбрать все лиды несколькими способами"""
        selectors = [
            ("ID CRM_LEAD_LIST_check_all", "//input[contains(@id, 'CRM_LEAD_LIST') and contains(@id, '_check_all')]"),
            ("Класс main-grid-check-all", "//input[contains(@class, 'main-grid-check-all')]"),
            ("Заголовок 'Отметить все'", "//input[@type='checkbox' and contains(@title, 'Отметить все')]"),
            ("Последний чекбокс в заголовке", "//thead//input[@type='checkbox']")
        ]

        for name, xpath in selectors:
            try:
                logger.info(f"Попытка выбора через {name}")
                elements = self.driver.find_elements(By.XPATH, xpath)
                if elements:
                    elements[0].click()
                    logger.info(f"Успешно выбрано через {name}")
                    return True
            except Exception as e:
                logger.warning(f"Не удалось выбрать через {name}: {str(e)}")

        logger.error("Не удалось выбрать все лиды ни одним из способов")
        return False

    @log_step("Запуск сценария обогащения")
    def run_enrichment_scenario(self):
        """Запуск сценария обогащения данных"""
        try:
            # Открытие меню сценариев
            self.driver.execute_script("window.scrollTo(0, document.body.scrollTop);")
            self.driver.find_element(
                By.XPATH, "//button[@id='intranet_binding_menu_crm_switcher']"
            ).click()
            time.sleep(1)

            # Выбор умных сценариев
            self.driver.find_element(
                By.XPATH, "//span[text()='Умные сценарии']"
            ).click()
            time.sleep(1)

            # Выбор сценария обогащения
            self.driver.find_element(
                By.XPATH, "//span[text()='Обогащение Лида через Checko И exportBase']"
            ).click()
            time.sleep(2)

            # Запуск сценария
            run_btn = self.driver.find_element(
                By.XPATH, "//span[@class='ui-btn-text-inner' and text()='Запустить']/parent::span/parent::button"
            )
            self.driver.execute_script("arguments[0].click();", run_btn)
            time.sleep(2)
            self.status = 'Сценарий обогащения данных запущен'
            logger.info("Умный сценарий успешно запущен")
        except Exception as e:
            logger.error(f"Ошибка при запуске сценария: {str(e)}")
            self.status = 'Ошибка при запуске сценария'
            raise

    def execute(self):
        """Основной метод выполнения процесса"""
        try:
            self.initialize()
            self.login()
            self.go_to_kanban()
            self.upload_file()
            self.configure_import()
            self.get_import_stats()

            # Переход на страницу лидов и запуск сценария в любом случае
            self.driver.get(os.getenv('BITRIX_LEADS_PAGE'))
            time.sleep(3)

            self.process_csv_file()
            self.setup_filters()

            if self.select_all_leads():
                self.run_enrichment_scenario()
            else:
                self.status = "Не удалось выбрать лиды, но попытка запуска сценария выполнена"
                self.run_enrichment_scenario()

            return self.status
        except Exception as e:
            logger.error(f"Критическая ошибка: {str(e)}")
            self.status = f"Ошибка: {str(e)}"
            return self.status
        finally:
            if hasattr(self, 'driver') and self.driver:
                try:
                    self.driver.quit()
                    logger.info("Браузер закрыт")
                except Exception as quit_error:
                    logger.error(f"Ошибка при закрытии браузера: {str(quit_error)}")


def bitrix_upload_file():
    """Основная функция для вызова извне"""
    uploader = BitrixUploader()
    return uploader.execute()


if __name__ == "__main__":
    result = bitrix_upload_file()
    logger.info(f"Итоговый статус: {result}")