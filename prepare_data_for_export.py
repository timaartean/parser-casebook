import re
import os
import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)

needed_headers = [
    'Номер дела',
    'Дата регистрации дела',
    'Категория спора',
    'Ссылка',
    'Суд',
    'Истец/Кредитор',
    'Ответчик/Должник',
    'ИНН Ответчика/Должника',
    'Исковые требования',
]
abs_path = os.getcwd()


def normalize_case_number(case_num):
    """Унифицированная нормализация номера дела - только цифры"""
    if not case_num:
        return ""
    return re.sub(r'[^\d]', '', str(case_num).strip())


def prepare_data(headers=False, mode='w'):
    scraped_cases = set()
    defendant_set = set()
    stats = {
        'rows_in_file': 0,
        'passed_filters': 0,
        'prepared_count': 0,
        'skipped_seen_before': 0,
        'skipped_empty_defendant': 0,
        'skipped_defendant_block': 0,
        'skipped_invalid_inn_range': 0
    }

    # Чтение файлов с гарантированным закрытием
    if os.path.exists('cases_num.txt'):
        with open(os.path.join(abs_path, 'cases_num.txt'), 'r', encoding='utf-8') as f:
            # Нормализуем номера (цифры только), даже если в файле уже так
            scraped_cases = {normalize_case_number(sc) for sc in f.readlines() if normalize_case_number(sc)}

    # Дополнительно исключаем дела из реестра уже импортированных в Bitrix
    registry_path = os.path.join(abs_path, 'processed_cases.json')
    if os.path.exists(registry_path):
        try:
            with open(registry_path, 'r', encoding='utf-8') as rf:
                arr = json.load(rf)
                if isinstance(arr, list):
                    for x in arr:
                        normalized_case = normalize_case_number(x)
                        if normalized_case:  # Проверяем, что номер не пустой
                            scraped_cases.add(normalized_case)
                    logger.info(f"Загружено {len(arr)} номеров дел из processed_cases.json для исключения")
        except Exception as e:
            logger.warning(f"Ошибка при чтении processed_cases.json: {e}")
            pass

    if os.path.exists('defendant.txt'):
        with open(os.path.join(abs_path, 'defendant.txt'), 'r', encoding='utf-8') as f:
            defendant_set = {li.strip().lower() for li in f.readlines()}

    # Чтение CSV
    raw_csv_path = os.path.join(abs_path, 'ArbitrageSearchExport.csv')
    data = pd.read_csv(
        raw_csv_path,
        sep=';', encoding='windows-1251', dtype=str).to_dict(orient='records')
    stats['rows_in_file'] = len(data)

    ready_data = []
    for line in data:
        new_line = {}
        for nh in needed_headers:
            new_line[nh] = line[nh]

        case_num = new_line['Номер дела']
        case_for_file = normalize_case_number(case_num)

        if case_for_file and case_for_file in scraped_cases:
            stats['skipped_seen_before'] += 1
            continue

        if not new_line.get('Ответчик/Должник') or not isinstance(new_line.get('Ответчик/Должник'), str):
            stats['skipped_empty_defendant'] += 1
            continue

        names_is_list = new_line['Ответчик/Должник'].split('\n')
        try:
            inns_list = new_line['ИНН Ответчика/Должника'].split('\n')
        except AttributeError:
            inns_list = []

        if len(names_is_list) > 1 and len(inns_list) > 1:
            for name_num, name in enumerate(names_is_list):
                new_line_copy = new_line.copy()
                name = name.replace('\r', '')
                name_inn = inns_list[name_num].replace('\r', '') if name_num < len(inns_list) else ''

                if '-' in name_inn:
                    stats['skipped_invalid_inn_range'] += 1
                    continue

                def_names_list = [token.lower() for token in name.replace('"', '').split()]
                has_block = any(token in defendant_set for token in def_names_list)

                if has_block:
                    stats['skipped_defendant_block'] += 1
                    continue

                new_line_copy['Ответчик/Должник'] = name
                new_line_copy['ИНН Ответчика/Должника'] = name_inn
                new_line_copy['Название лида'] = name
                ready_data.append(new_line_copy)
        else:
            def_names_list = [token.lower() for token in new_line['Ответчик/Должник'].replace('"', '').split()]
            has_block = any(token in defendant_set for token in def_names_list)

            if has_block:
                stats['skipped_defendant_block'] += 1
                continue

            new_line['Название лида'] = line['Ответчик/Должник']
            ready_data.append(new_line)

    stats['passed_filters'] = len(ready_data)

    if ready_data:
        cleaned_path = os.path.join(abs_path, 'CleanedArbitrage.csv')

        # Дедупликация внутри батча строго по номеру дела
        df_new = pd.DataFrame(ready_data)
        df_new['__case__'] = df_new['Номер дела'].astype(str).apply(normalize_case_number)
        df_new = df_new.loc[~df_new['__case__'].duplicated()].copy()

        # Не накапливаем прошлые проходы: всегда сохраняем ТОЛЬКО текущие новые дела
        if '__case__' in df_new.columns:
            df_new = df_new.drop(columns=['__case__'])
        stats['prepared_count'] = len(df_new)

        effective_header = headers
        if mode == 'a' and not os.path.exists(cleaned_path):
            effective_header = True

        df_new.to_csv(
            cleaned_path,
            sep=';',
            index=False,
            encoding='windows-1251',
            mode=mode,
            header=effective_header
        )
        # После успешной записи удаляем исходный файл
        try:
            if os.path.exists(raw_csv_path):
                os.remove(raw_csv_path)
        except Exception as rm_err:
            logger.warning(f"Не удалось удалить ArbitrageSearchExport.csv: {rm_err}")
        logger.info('Data is ready')
        prepare_data.last_stats = stats
        return True
    else:
        logger.info('No new data since last time')
        prepare_data.last_stats = stats
        return False


prepare_data.last_stats = {}
