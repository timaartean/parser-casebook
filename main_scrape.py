import os
import ast
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
import bitrix_upload_data as upload_data
import casebook_download_data as get_data
import prepare_data_for_export as set_data
from schedule import every, repeat, run_pending
import subprocess
import pandas as pd

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    filename='app.log',
                    filemode='a',
                    format="{asctime} - {filename} - {levelname} - {message}",
                    datefmt="%Y-%m-%d %H:%M",
                    style="{")

# Логгер для основного сценария
logger = logging.getLogger('MainScrape')
logger.setLevel(logging.INFO)
if not logger.handlers:
    _sh = logging.StreamHandler()
    _sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(_sh)

SUMMARY_LOG_PATH = 'pipeline_summary.log'


def kill_chrome_processes():
    """Убить все процессы Chrome"""
    try:
        subprocess.run(['pkill', '-f', 'chrome'], check=False)
        subprocess.run(['pkill', '-f', 'chromedriver'], check=False)
        time.sleep(2)
    except Exception as e:
        logging.warning(f"Не удалось убить процессы Chrome: {str(e)}")


def cleanup_system():
    """Очистка системы перед запуском"""
    kill_chrome_processes()
    time.sleep(3)  # Добавьте небольшую паузу после убийства процессов

    # Очистка временных файлов текущего прогона
    temp_files = ['ArbitrageSearchExport.csv', 'CleanedArbitrage.csv']
    for file in temp_files:
        if os.path.exists(file):
            try:
                os.remove(file)
            except:
                pass

@repeat(every().hour)
def check_courts():
    current_hour = datetime.now().strftime('%H')
    bad_times = ['22', '23', '00', '01', '02', '03', '04', '05', '06']
    requests_bundled = ast.literal_eval(os.environ['REQUESTS_BUNDLED'])
    abs_path = os.getcwd()

    if current_hour not in bad_times:
        # Очистка перед запуском
        cleanup_system()

        run_bitrix_scenario = False
        headers = True
        today = datetime.now().strftime('%d.%m.%Y')
        mode = 'w'

        total_reqs = len(requests_bundled)
        summary_data = {
            'requests_total': total_reqs,
            'requests_attempted': 0,
            'casebook_found': 0,
            'casebook_downloaded': 0,
            'failed_download_results': 0,
            'csv_rows_total': 0,
            'passed_filters': 0,
            'prepared_rows': 0,
            'skipped_seen': 0,
            'skipped_defendant': 0,
            'skipped_empty_defendant': 0,
            'skipped_invalid_inn': 0,
            'prepared_file_unique': 0,
            'bitrix_status': 'Bitrix не запущен',
            'bitrix_created': None,
            'bitrix_updated': None
        }

        # Открываем одну сессию браузера Casebook для всех запросов
        downloader = None
        try:
            downloader = get_data.create_casebook_session()

            for req_idx, bundle in enumerate(requests_bundled):
                try:
                    court = bundle[0]
                    category_code = bundle[1]
                    min_sum = bundle[2]
                    date_from_opt = bundle[3] if len(bundle) > 3 else None
                except Exception:
                    print(f"Некорректный формат REQUESTS_BUNDLED в элементе {bundle}, пропуск")
                    continue

                court_type = f"\n\tСуд в деле: {court}"
                category = f"\n\tКатегория спора: {category_code}"
                # Если в bundle передана дата, используем её для обеих границ
                env_date_val = (os.getenv('CASEBOOK_DATE') or '').strip()
                env_date_from_val = (os.getenv('CASEBOOK_DATE_FROM') or '').strip()
                env_date_to_val = (os.getenv('CASEBOOK_DATE_TO') or '').strip()

                if date_from_opt and str(date_from_opt).strip():
                    eff_from = eff_to = str(date_from_opt).strip()
                elif env_date_val:
                    eff_from = eff_to = env_date_val
                elif env_date_from_val or env_date_to_val:
                    eff_from = env_date_from_val or env_date_to_val
                    eff_to = env_date_to_val or env_date_from_val
                else:
                    eff_from = eff_to = datetime.now().strftime('%d.%m.%Y')

                from_date = f"\n\tДата регистрации дела с: {eff_from}"
                to_date = f"\n\tДата регистрации дела по: {eff_to}"
                min_summ = f"\n\tИсковые требования в деле от: {min_sum}"

                last_params = f'{from_date}{to_date}{min_summ}\n'
                params = f'\n{court_type}{category}{last_params}'
                progress = f"Проход {req_idx + 1}/{total_reqs}"
                logger.info(progress)
                logger.info(f'Выгрузка дел со следующими параметрами: {params}')

                summary_data['requests_attempted'] += 1
                last_results_count = 0
                download_success = False
                attempt_num = 0
                while attempt_num < 3:
                    try:
                        if downloader is None:
                            downloader = get_data.create_casebook_session()
                        downloaded, results_count = get_data.process_casebook_request(
                            downloader, court, category_code, min_sum, date_from_opt
                        )
                        last_results_count = results_count or 0
                        if downloaded:
                            download_success = True
                            summary_data['casebook_found'] += last_results_count
                            summary_data['casebook_downloaded'] += last_results_count
                            logger.info(f'{progress}: Подготовка лидов...')
                            if req_idx > 0:
                                headers = False
                                mode = 'a'
                            got_new_leads = set_data.prepare_data(headers=headers, mode=mode)
                            prepare_stats = getattr(set_data.prepare_data, 'last_stats', {}) or {}
                            summary_data['csv_rows_total'] += prepare_stats.get('rows_in_file', 0) or 0
                            summary_data['passed_filters'] += prepare_stats.get('passed_filters', 0) or 0
                            summary_data['prepared_rows'] += prepare_stats.get('prepared_count', 0) or 0
                            summary_data['skipped_seen'] += prepare_stats.get('skipped_seen_before', 0) or 0
                            summary_data['skipped_defendant'] += prepare_stats.get('skipped_defendant_block', 0) or 0
                            summary_data['skipped_empty_defendant'] += prepare_stats.get('skipped_empty_defendant', 0) or 0
                            summary_data['skipped_invalid_inn'] += prepare_stats.get('skipped_invalid_inn_range', 0) or 0
                            if got_new_leads:
                                run_bitrix_scenario = True
                            else:
                                logger.info(f'✅ {progress}: Новых лидов нет')
                            logger.info(f'✅ {progress}: завершён успешно')
                            break
                        else:
                            attempt_num += 1
                            if results_count == 0:
                                summary_data['casebook_found'] += last_results_count
                                logger.info(f'✅ {progress}: Найдено 0️⃣ арбитражных дел')
                                break
                            else:
                                logger.warning(f"{progress}: Не удалось скачать по параметрам: {params}")
                    except Exception as e:
                        attempt_num += 1
                        logger.error(f'{progress}: Ошибка при обработке запроса {bundle}: {str(e)}')
                        # Перезапускаем сессию браузера и продолжаем с того же бандла
                        try:
                            if downloader is not None:
                                get_data.close_casebook_session(downloader)
                        except Exception:
                            pass
                        downloader = None
                        time.sleep(2)
                        try:
                            downloader = get_data.create_casebook_session()
                        except Exception as se:
                            logger.error(f'{progress}: Ошибка при создании новой сессии: {str(se)}')
                            time.sleep(10)
                        # Продолжаем цикл; накопленные файлы не трогаем
                        time.sleep(5)
                if not download_success and last_results_count:
                    summary_data['casebook_found'] += last_results_count
                    summary_data['failed_download_results'] += last_results_count
        finally:
            if downloader is not None:
                get_data.close_casebook_session(downloader)

        bitrix_stats = {}
        if run_bitrix_scenario:
            # Подсчет общего количества дел по уникальному номеру дела
            total_prepared = 0
            try:
                cleaned_path = os.path.join(abs_path, 'CleanedArbitrage.csv')
                if os.path.exists(cleaned_path):
                    df = pd.read_csv(cleaned_path, sep=';', encoding='windows-1251')
                    if 'Номер дела' in df.columns:
                        df['__case__'] = df['Номер дела'].astype(str)
                        total_prepared = df['__case__'].nunique()
                    else:
                        total_prepared = len(df)
            except Exception as te:
                logger.warning(f'Не удалось посчитать количество дел для выгрузки: {te}')

            summary_data['prepared_file_unique'] = total_prepared
            logger.info(f"Всего дел для выгрузки в Bitrix: {total_prepared}")
            logger.info('Импорт лидов...')
            upload_status = upload_data.bitrix_upload_file()
            logger.info(f"Импорт завершён: {upload_status}")
            bitrix_stats = getattr(upload_data.bitrix_upload_file, 'last_stats', {}) or {}
            summary_data['bitrix_status'] = bitrix_stats.get('status', upload_status)
            summary_data['bitrix_created'] = bitrix_stats.get('created_leads')
            summary_data['bitrix_updated'] = bitrix_stats.get('updated_leads')

        # Принудительная очистка после выполнения
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        bitrix_created_display = summary_data['bitrix_created'] if summary_data['bitrix_created'] is not None else 'n/a'
        bitrix_updated_display = summary_data['bitrix_updated'] if summary_data['bitrix_updated'] is not None else 'n/a'
        summary_line = (
            f"{timestamp} | requests={summary_data['requests_attempted']}/{summary_data['requests_total']} | "
            f"found={summary_data['casebook_found']} | downloaded={summary_data['casebook_downloaded']} "
            f"(csv_rows={summary_data['csv_rows_total']}) | passed_filters={summary_data['passed_filters']} | "
            f"prepared_written={summary_data['prepared_rows']} | unique_for_bitrix={summary_data['prepared_file_unique']} | "
            f"bitrix_created={bitrix_created_display} | bitrix_updated={bitrix_updated_display} | "
            f"bitrix_status={summary_data['bitrix_status']} | skipped_seen={summary_data['skipped_seen']} | "
            f"skipped_defendant={summary_data['skipped_defendant']} | skipped_empty={summary_data['skipped_empty_defendant']} | "
            f"skipped_invalid_inn={summary_data['skipped_invalid_inn']} | failed_download={summary_data['failed_download_results']}"
        )
        try:
            with open(SUMMARY_LOG_PATH, 'a', encoding='utf-8') as summary_file:
                summary_file.write(summary_line + '\n')
        except Exception as write_err:
            logger.warning(f'Не удалось записать сводку в {SUMMARY_LOG_PATH}: {write_err}')
        logger.info(f'Сводка прогона: {summary_line}')

        cleanup_system()
    else:
        logging.info(f'Текущее время {current_hour} - тихий час, пропуск')



if __name__ == "__main__":
    # Очистка при запуске
    cleanup_system()

    check_courts()
    while True:
        run_pending()
        time.sleep(60)  # Увеличено до 60 секунд
