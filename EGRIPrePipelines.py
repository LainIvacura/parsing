import xml.dom.minidom as minidom
from support_citadel import ConnectionFactory
import zipfile
import os
import time
import httpx
from config import LOG_FILE, RELOAD_SCRIPT_PATH_conf, PACKETS_BASE_PATH_conf, ARCHIVES_BASE_PATH_conf

cf = ConnectionFactory()

# Настройки
BATCH_SIZE = 1000
SLEEP_BETWEEN_ITERATIONS = 3
BATCH_MISMATCH_SIZE = 400  # Количество несоответствий для одного пайплайна

# Диапазон дат для выборки пакетов
DATE_FROM = '2026-07-06 00:00:00'
DATE_TO   = '2026-07-06 23:59:59'

# Пути
RELOAD_SCRIPT_PATH = RELOAD_SCRIPT_PATH_conf
PACKETS_BASE_PATH  = PACKETS_BASE_PATH_conf
ARCHIVES_BASE_PATH = ARCHIVES_BASE_PATH_conf

# Лог файл
filename = LOG_FILE

# GitLab конфигурация
GITLAB_API_URL = os.getenv("GITLAB_API_URL")
GITLAB_PROJECT_ID = os.getenv("GITLAB_PROJECT_ID")
GITLAB_TRIGGER_TOKEN = os.getenv("GITLAB_TRIGGER_TOKEN")
GITLAB_BRANCH  = os.getenv("GITLAB_BRANCH")

# Подключаемся к БД
connect_sectionks = cf.get_connection('section')
oos_integration_packet_table = connect_sectionks.get_table('oosIntegrationPacket')
organization_table = connect_sectionks.get_table('organization')
organization_document_table = connect_sectionks.get_table('organizationDocument')

connect_support = cf.get_connection('supp_base')
table_SUPPORT = connect_support.get_table('max_service_ids')

# Кэш для проверенных организаций (храним registry_num)
checked_organizations = set()


def mark_organization_as_checked(registry_num):
    """Запоминаем, что организация уже проверена"""
    checked_organizations.add(registry_num)


def is_organization_checked(registry_num):
    """Проверяем, проверялась ли уже эта организация"""
    return registry_num in checked_organizations


# СИНХРОННАЯ версия запроса к GitLab (без asyncio)
def trigger_reload_organizations_sync(org_ids: list) -> dict:
    """Синхронный запуск пайплайна для перекачки данных списка организаций"""
    url = f"{GITLAB_API_URL}/projects/{GITLAB_PROJECT_ID}/trigger/pipeline"

    org_ids_str = ','.join(str(org_id) for org_id in org_ids)
    script_command = f"{RELOAD_SCRIPT_PATH} --org {org_ids_str}"

    data = {
        "token": GITLAB_TRIGGER_TOKEN,
        "ref": GITLAB_BRANCH,
        "variables[CI_PIPELINE_SOURCE]": "api",
        "variables[SERVICE_NAME]": "section",
        "variables[CUSTOM_SCRIPT_PATH]": script_command
    }

    try:
        # Используем синхронный httpx вместо асинхронного
        with httpx.Client(timeout=60.0) as client:
            response = client.post(url, data=data)

            if response.status_code in [200, 201]:
                result = response.json()
                return {
                    "success": True,
                    "pipeline_id": result.get("id"),
                    "pipeline_url": result.get("web_url"),
                    "org_ids": org_ids
                }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text[:500]}",
                    "org_ids": org_ids
                }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "org_ids": org_ids
        }


def extract_fio_from_zip(zip_path):
    """Читает XML файлы из ZIP архива без распаковки и извлекает данные для сравнения"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for file_name in zf.namelist():
                if file_name.lower().endswith('.xml'):
                    with zf.open(file_name) as xml_file:
                        content = xml_file.read().decode('utf-8')
                        if content.lstrip().startswith('<?xml'):
                            src = minidom.parseString(content)
                            root_element = src.documentElement
                            type_info = root_element.getAttribute('ТипИнф')

                            if type_info == 'ЕГРИП_ОГР_СВЕД':
                                svip_nodes = src.getElementsByTagName('СвИП')
                                for svip in svip_nodes:
                                    svfl_nodes = svip.getElementsByTagName('СвФЛ')
                                    for svfl in svfl_nodes:
                                        fio_nodes = svfl.getElementsByTagName('ФИОРус')
                                        for fio in fio_nodes:
                                            last_name = fio.getAttribute('Фамилия')
                                            first_name = fio.getAttribute('Имя')
                                            middle_name = fio.getAttribute('Отчество')
                                            if last_name and first_name:
                                                return f"{last_name} {first_name} {middle_name}".strip()

                            elif type_info == 'ЕГРЮЛ_ОГР_СВЕД':
                                name_nodes = src.getElementsByTagName('СвНаимЮЛ')
                                for name_node in name_nodes:
                                    full_name = name_node.getAttribute('НаимЮЛПолн')
                                    if full_name:
                                        return full_name.strip()
        return None
    except Exception as e:
        return None


def normalize_name(name):
    if not name:
        return ""
    return ' '.join(name.upper().split())


def get_last_processed_id():
    """Получает последний обработанный ID пакета"""
    with connect_support.get_session() as sess:
        support_info = sess.query(table_SUPPORT).filter(
            table_SUPPORT.service == 'reDownloadEGRULEGRIP'
        ).first()
        if support_info and support_info.value:
            return int(support_info.value)
        return 0


def update_last_processed_id(last_id):
    """Обновляет последний обработанный ID пакета"""
    with connect_support.get_session() as sess:
        support_info = sess.query(table_SUPPORT).filter(
            table_SUPPORT.service == 'reDownloadEGRULEGRIP'
        ).first()
        if support_info:
            support_info.value = str(last_id)
            sess.commit()
        else:
            # Создаем запись если её нет
            support_info = table_SUPPORT(
                service='reDownloadEGRULEGRIP',
                value=str(last_id)
            )
            sess.add(support_info)
            sess.commit()


# ОСНОВНАЯ ЛОГИКА
last_processed_id = get_last_processed_id()
mismatches_buffer = []  # Буфер для сбора несоответствий

# ИСПОЛЬЗУЕМ ОДНУ СЕССИЮ ДЛЯ ВСЕГО ЦИКЛА
with connect_sectionks.get_session() as sess:
    while True:
        print(f"Загружаем пакеты с ID > {last_processed_id}...")

        # Получаем пакеты
        packets = sess.query(
            oos_integration_packet_table.id,
            oos_integration_packet_table.contentUri
        ).filter(
            oos_integration_packet_table.typeId == '221',
            oos_integration_packet_table.createDateTime.between(DATE_FROM, DATE_TO),
            oos_integration_packet_table.id > last_processed_id
        ).limit(BATCH_SIZE).all()

        if not packets:
            # Обрабатываем остатки в буфере перед завершением
            if mismatches_buffer:
                print(f"Завершаем работу. Обработка последних {len(mismatches_buffer)} несоответствий...")
                with open(filename, 'a', encoding='utf-8') as log_file:
                    now = time.strftime("%Y-%m-%d %H:%M:%S")
                    log_file.write(f"\n{now} - ПАЧКА {len(mismatches_buffer)} НЕСООТВЕТСТВИЙ ПЕРЕД ЗАВЕРШЕНИЕМ\n")

                org_ids = [m['org_id'] for m in mismatches_buffer]
                pipeline_result = trigger_reload_organizations_sync(org_ids)

                with open(filename, 'a', encoding='utf-8') as log_file:
                    if pipeline_result.get("success"):
                        log_file.write(
                            f"  Пайплайн запущен успешно! ID: {pipeline_result.get('pipeline_id')}, URL: {pipeline_result.get('pipeline_url')}\n")
                        log_file.write(f"  Организации: {', '.join(str(oid) for oid in org_ids)}\n\n")
                        print(f"  Пайплайн запущен успешно для {len(org_ids)} организаций!")
                    else:
                        log_file.write(f"  ОШИБКА запуска пайплайна: {pipeline_result.get('error')}\n\n")
                        print(f"  ОШИБКА запуска пайплайна: {pipeline_result.get('error')}")

            print("Нет больше пакетов для обработки")
            break

        print(f"Загружено {len(packets)} пакетов")

        # Сохраняем ID всех пакетов в пачке
        packet_ids = [packet.id for packet in packets]

        # Обрабатываем каждый пакет (используем ту же сессию)
        for packet in packets:
            packet_id = packet.id
            contentUri = packet.contentUri

            if not contentUri:
                continue

            file_path = contentUri.replace('.', PACKETS_BASE_PATH)

            try:
                with open(file_path, encoding="utf-8") as f:
                    content = f.read()
                    if not content.lstrip().startswith('<?xml'):
                        continue

                    src = minidom.parseString(content)
                    registry_num_nodes = src.getElementsByTagName('registryNum')

                    if len(registry_num_nodes) == 0:
                        continue

                    registry_node = registry_num_nodes[0]
                    if not (registry_node.firstChild and registry_node.firstChild.nodeValue):
                        continue

                    registry_num = registry_node.firstChild.nodeValue.strip()

            except Exception as e:
                print(f"Ошибка при чтении {file_path}: {str(e)}")
                continue

            # Проверяем, проверяли ли уже эту организацию
            if is_organization_checked(registry_num):
                continue

            result = sess.query(
                organization_table.id,
                organization_table.inn,
                organization_table.fullName,
                organization_document_table.uri
            ).join(
                organization_document_table,
                organization_table.id == organization_document_table.organizationId
            ).filter(
                organization_table.oosRegistrationNumber == registry_num,
                organization_table.type == 'supplier',
                organization_table.active == 1,
                organization_document_table.typeId.in_(['16', '9'])
            ).all()

            if not result:
                mark_organization_as_checked(registry_num)
                continue

            org_id = result[0][0]
            org_inn = result[0][1]
            org_full_name = result[0][2]

            zip_files = []
            for row in result:
                uri = row[3]
                if uri and uri.endswith('.zip'):
                    zip_path = uri.replace('local://', ARCHIVES_BASE_PATH)
                    if os.path.exists(zip_path):
                        zip_files.append(zip_path)

            if not zip_files:
                mark_organization_as_checked(registry_num)
                continue

            fio_from_archive = None
            for zip_path in zip_files:
                fio_from_archive = extract_fio_from_zip(zip_path)
                if fio_from_archive:
                    break

            if not fio_from_archive:
                mark_organization_as_checked(registry_num)
                continue

            db_name_normalized = normalize_name(org_full_name)
            archive_name_normalized = normalize_name(fio_from_archive)

            # Отмечаем как проверенную (независимо от результата)
            mark_organization_as_checked(registry_num)

            if db_name_normalized != archive_name_normalized:
                # Сохраняем информацию о несоответствии в буфер
                mismatches_buffer.append({
                    'packet_id': packet_id,
                    'org_id': org_id,
                    'org_inn': org_inn,
                    'org_full_name': org_full_name,
                    'fio_from_archive': fio_from_archive
                })

                # Логируем несоответствие
                with open(filename, 'a', encoding='utf-8') as log_file:
                    now = time.strftime("%Y-%m-%d %H:%M:%S")
                    log_file.write(f"{now} - ID пакета: {packet_id}\n")
                    log_file.write(f"  ИНН: {org_inn}\n")
                    log_file.write(f"  ID организации: {org_id}\n")
                    log_file.write(f"  fullName: {org_full_name}\n")
                    log_file.write(f"  ФИО из ZIP: {fio_from_archive}\n")
                    log_file.write(f"  Вердикт: НЕ СОВПАДАЕТ\n")
                    log_file.write(f"  Добавлен в буфер ({len(mismatches_buffer)}/{BATCH_MISMATCH_SIZE})\n\n")

                print(
                    f"  Найдено несоответствие для org_id={org_id}. Буфер: {len(mismatches_buffer)}/{BATCH_MISMATCH_SIZE}")

                # Если буфер заполнился, запускаем пайплайн
                if len(mismatches_buffer) >= BATCH_MISMATCH_SIZE:
                    with open(filename, 'a', encoding='utf-8') as log_file:
                        log_file.write(f"\n--- ЗАПУСК ПАЙПЛАЙНА ДЛЯ {len(mismatches_buffer)} ОРГАНИЗАЦИЙ ---\n")

                    org_ids = [m['org_id'] for m in mismatches_buffer]
                    pipeline_result = trigger_reload_organizations_sync(org_ids)

                    with open(filename, 'a', encoding='utf-8') as log_file:
                        if pipeline_result.get("success"):
                            log_file.write(
                                f"  Пайплайн запущен успешно! ID: {pipeline_result.get('pipeline_id')}, URL: {pipeline_result.get('pipeline_url')}\n")
                            log_file.write(f"  Организации: {', '.join(str(oid) for oid in org_ids)}\n\n")
                            print(
                                f"  Пайплайн запущен успешно для {len(org_ids)} организаций! ID: {pipeline_result.get('pipeline_id')}")
                        else:
                            log_file.write(f"  ОШИБКА запуска пайплайна: {pipeline_result.get('error')}\n\n")
                            print(f"  ОШИБКА запуска пайплайна: {pipeline_result.get('error')}")

                    # Очищаем буфер
                    mismatches_buffer = []

                    # Коммитим изменения в БД после успешной обработки пачки
                    sess.commit()

        # После обработки всей пачки обновляем last_processed_id
        if packet_ids:
            max_id = max(packet_ids)
            update_last_processed_id(max_id)
            last_processed_id = max_id
            print(f"Обновлен last_processed_id: {last_processed_id}")

        # Коммитим изменения после каждой пачки пакетов
        sess.commit()

        if SLEEP_BETWEEN_ITERATIONS > 0:
            print(f"Пауза {SLEEP_BETWEEN_ITERATIONS} секунд...")
            time.sleep(SLEEP_BETWEEN_ITERATIONS)

print("Скрипт завершил работу")
