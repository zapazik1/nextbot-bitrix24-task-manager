

def get_webhook_from_sheet(sheet_url, user_name):
    """
    Получает вебхук пользователя из опубликованной Google Таблицы CSV.
    Формат таблицы: A - Имя, B - Вебхук.
    """
    debug(f"-> get_webhook_from_sheet: ищем вебхук для '{user_name}'")
    try:
        response = requests.get(sheet_url, timeout=5)
        response.raise_for_status()
        csv_data = response.text
        lines = csv_data.strip().splitlines()

        for line in lines:
            if not line.strip(): continue
            parts = line.strip().split(',')
            if len(parts) >= 2:
                sheet_user = parts[0].strip()
                webhook = parts[1].strip()
                webhook = ''.join(c for c in webhook if c.isprintable())
                
                if sheet_user == user_name:
                    debug(f"<- get_webhook_from_sheet: Вебхук для '{user_name}' найден.")
                    if not webhook.endswith('/'): webhook += '/'
                    return webhook
        
        debug(f"<- get_webhook_from_sheet: Пользователь '{user_name}' НЕ найден в таблице.")
        return None
    except Exception as e:
        debug(f"<- get_webhook_from_sheet: Ошибка при доступе к Google Sheets: {e}")
        return None

def find_task_id_by_title(webhook_url, title, project_id=None):
    """
    Ищет ID задачи в Bitrix24 по наиболее похожему названию (метод 'мешка слов').
    Завершенные задачи (статус 5) исключаются из поиска.
    Если указан project_id, ищет только в этом проекте.
    """
    debug(f"-> find_task_id_by_title (fuzzy): '{title}', project_id: {project_id}")
    url = f"{webhook_url}tasks.task.list.json"
    
    # Формируем фильтр
    task_filter = {"ZOMBIE": "N", "!STATUS": 5}
    if project_id is not None:
        task_filter["GROUP_ID"] = project_id
        
    # Получаем все активные и не завершенные задачи
    params = {"filter": task_filter, "select": ["ID", "TITLE"]}

    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        tasks = response.json().get("result", {}).get("tasks", [])

        if not tasks:
            debug("<- find_task_id_by_title (fuzzy): Не найдено ни одной активной задачи.")
            return None

        # Нормализация и разбиение на слова поискового запроса
        if not isinstance(title, str) or not title:
            search_words = set()
        else:
            cleaned_title = re.sub(r'[^\w\s]', '', title).lower()
            search_words = set(cleaned_title.split())

        if not search_words:
            debug("<- find_task_id_by_title (fuzzy): Поисковый запрос пуст после нормализации.")
            return None

        best_match_id = None
        max_common_count = 0

        for task in tasks:
            task_title = task.get("title")
            
            # Нормализация и разбиение на слова названия задачи
            if not isinstance(task_title, str) or not task_title:
                task_words = set()
            else:
                cleaned_task_title = re.sub(r'[^\w\s]', '', task_title).lower()
                task_words = set(cleaned_task_title.split())
            
            common_count = len(search_words.intersection(task_words))
            
            # Мы ищем задачу, у которой больше всего общих слов с поисковым запросом
            if common_count > max_common_count:
                max_common_count = common_count
                best_match_id = int(task.get("id"))
                debug(f"Новый лучший кандидат: ID {best_match_id} ('{task_title}') с {common_count} совпадениями.")

        if best_match_id and max_common_count > 0:
            debug(f"<- find_task_id_by_title (fuzzy): Найдена наиболее похожая задача ID: {best_match_id}")
        else:
            debug(f"<- find_task_id_by_title (fuzzy): Не найдено похожих задач для '{title}'.")
            best_match_id = None
        
        return best_match_id

    except requests.exceptions.RequestException as e:
        debug(f"<- find_task_id_by_title: ОШИБКА API: {e}")
    except Exception as e:
        debug(f"<- find_task_id_by_title: Непредвиденная ошибка: {e}")
    return None


def update_b24_task(webhook_url, task_id, fields):
    """Обновляет задачу в Bitrix24 и возвращает ее ID и ссылку."""
    debug(f"-> update_b24_task: ID={task_id}, Поля={fields}")
    url = f"{webhook_url}tasks.task.update.json"
    params = {"taskId": task_id, "fields": fields}
    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        result_json = response.json()
        if "result" in result_json and "task" in result_json["result"]:
            task = result_json["result"]["task"]
            updated_id = task.get("id")
            if updated_id:
                portal_url = webhook_url.split('/rest/')[0]
                creator_id = task.get("createdBy")
                task_link = f"{portal_url}/company/personal/user/{creator_id}/tasks/task/view/{updated_id}/"
                debug(f"<- update_b24_task: Задача обновлена, ID: {updated_id}")
                return int(updated_id), task_link
    except requests.exceptions.RequestException as e:
        debug(f"<- update_b24_task: ОШИБКА API: {e}")
        if 'response' in locals() and hasattr(response, 'text'):
            debug(f"Ответ от сервера: {response.text}")
    except Exception as e:
        debug(f"<- update_b24_task: Непредвиденная ошибка: {e}")
    
    debug("<- update_b24_task: Не удалось обновить задачу.")
    return None, None


def parse_deadline(deadline_str):
    """Преобразует текстовое описание срока в формат Bitrix24."""
    debug(f"-> parse_deadline: '{deadline_str}'")
    deadline_str = deadline_str.lower().strip()
    now = datetime.datetime.now()
    deadline_dt = None

    if "завтра" in deadline_str:
        deadline_dt = (now + datetime.timedelta(days=1)).replace(hour=18, minute=0, second=0)
    elif "послезавтра" in deadline_str:
        deadline_dt = (now + datetime.timedelta(days=2)).replace(hour=18, minute=0, second=0)
    elif "через неделю" in deadline_str:
        deadline_dt = (now + datetime.timedelta(weeks=1)).replace(hour=18, minute=0, second=0)
    elif "через" in deadline_str:
        match = re.search(r"через (\d+)\s+(дн|дня|дней|час|часа|часов)", deadline_str)
        if match:
            parts = match.groups()
            value = int(parts[0])
            unit = parts[1]
            if "дн" in unit:
                deadline_dt = (now + datetime.timedelta(days=value)).replace(hour=18, minute=0, second=0)
            elif "час" in unit:
                deadline_dt = now + datetime.timedelta(hours=value)
    else:
        try:
            # Ручной парсинг "ДД.ММ.ГГГГ" или "ДД.ММ.ГГГГ ЧЧ:ММ"
            parts = deadline_str.split(' ')
            date_part = parts[0]
            date_parts = date_part.split('.')
            day = int(date_parts[0])
            month = int(date_parts[1])
            year = int(date_parts[2])
            hour = 18
            minute = 0
            if len(parts) > 1:
                time_part = parts[1]
                time_parts = time_part.split(':')
                if len(time_parts) >= 2:
                    hour = int(time_parts[0])
                    minute = int(time_parts[1])
            deadline_dt = datetime.datetime(year, month, day, hour, minute)
        except (ValueError, IndexError):
            debug(f"Не удалось преобразовать '{deadline_str}' в дату и время.")
            deadline_dt = None

    if deadline_dt:
        # Ручное форматирование даты в строку
        y = deadline_dt.year
        m = deadline_dt.month
        d = deadline_dt.day
        h = deadline_dt.hour
        mi = deadline_dt.minute
        s = deadline_dt.second
        result_dt = f"{y:04d}-{m:02d}-{d:02d}T{h:02d}:{mi:02d}:{s:02d}"
        debug(f"<- parse_deadline: возвращает '{result_dt}'")
        return result_dt

    debug(f"<- parse_deadline: не удалось распознать срок, возвращает None")
    return None


def find_project_id_by_name(webhook_url, project_name):
    """Ищет ID проекта (рабочей группы) в Bitrix24 по наиболее похожему названию (метод 'мешка слов')."""
    debug(f"-> find_project_id_by_name (fuzzy): '{project_name}'")
    url = f"{webhook_url}sonet_group.get.json"
    # Получаем все проекты
    params = {} 

    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        projects = response.json().get("result", [])

        if not projects:
            debug("<- find_project_id_by_name (fuzzy): Список проектов пуст.")
            return None
            
        # Нормализация и разбиение на слова поискового запроса
        if not isinstance(project_name, str) or not project_name:
            search_words = set()
        else:
            cleaned_project_name = re.sub(r'[^\w\s]', '', project_name).lower()
            search_words = set(cleaned_project_name.split())
        
        if not search_words:
            debug("<- find_project_id_by_name (fuzzy): Название проекта пустое после нормализации.")
            return None

        best_match_id = None
        max_common_count = 0

        for project in projects:
            current_project_name = project.get("NAME")

            # Нормализация и разбиение на слова названия проекта
            if not isinstance(current_project_name, str) or not current_project_name:
                project_words = set()
            else:
                cleaned_current_project_name = re.sub(r'[^\w\s]', '', current_project_name).lower()
                project_words = set(cleaned_current_project_name.split())
            
            common_count = len(search_words.intersection(project_words))
            
            if common_count > max_common_count:
                max_common_count = common_count
                best_match_id = int(project.get("ID"))
                debug(f"Новый лучший кандидат на проект: ID {best_match_id} ('{current_project_name}') с {common_count} совпадениями.")

        # Считаем совпадение успешным, если нашлось хотя бы одно общее слово
        if best_match_id and max_common_count > 0:
            debug(f"<- find_project_id_by_name (fuzzy): Найден наиболее похожий проект ID: {best_match_id}")
            return best_match_id
        else:
            debug(f"<- find_project_id_by_name (fuzzy): Не найдено достаточно похожего проекта для '{project_name}'.")
            return None

    except requests.exceptions.RequestException as e:
        debug(f"<- find_project_id_by_name (fuzzy): ОШИБКА API: {e}")
    return None


def find_user_id_by_name(webhook_url, user_name):
    """Ищет ID пользователя в Bitrix24 по имени/фамилии."""
    url = f"{webhook_url}user.search.json"
    params = {"FILTER": {"FIND": user_name}}
    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        result_json = response.json()
        if result_json.get("result") and len(result_json["result"]) > 0:
            return int(result_json["result"][0].get("ID"))
    except Exception:
        pass
    return None

# --- Основная функция, которую вызывает платформа ---

def main(args):
    """
    Основная логика обновления существующей задачи в Bitrix24.
    Ищет задачу по 'find_title', а затем обновляет переданные поля.
    """
    debug("--- Запуск функции update_task ---")
    debug(f"Получены аргументы от NextBot: {args}")

    GSHEET_URL = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/pub?gid=0&single=true&output=csv" 

    user_name = args.get("nameUser")
    if not user_name:
        msg = {"result": "error", "message": "Техническая ошибка: не было передано имя пользователя (nameUser)."}
        debug(f"ОШИБКА: {msg['message']}")
        return json.dumps(msg, ensure_ascii=False)

    webhook_url = get_webhook_from_sheet(GSHEET_URL, user_name)
    if not webhook_url:
        msg = {"result": "error", "message": f"Не удалось найти вебхук для пользователя '{user_name}'. Убедитесь, что вы внесены в базу и таблица опубликована."}
        debug(f"ОШИБКА: {msg['message']}")
        return json.dumps(msg, ensure_ascii=False)
    
    find_title = args.get("find_title")

    if not find_title:
        msg = {"result": "error", "message": "Необходимо указать 'find_title' для поиска задачи."}
        debug(f"ОШИБКА: {msg['message']}")
        return json.dumps(msg, ensure_ascii=False)

    project_name = args.get("project")
    project_id = None

    if project_name:
        debug(f"Поиск проекта по названию: '{project_name}'")
        project_id = find_project_id_by_name(webhook_url, project_name)
        if not project_id:
            msg = {"result": "error", "message": f"Проект с названием, похожим на '{project_name}', не найден. Обновление отменено."}
            debug(f"ОШИБКА: {msg['message']}")
            return json.dumps(msg, ensure_ascii=False)
        debug(f"Проект найден. ID: {project_id}. Поиск задачи будет в этом проекте.")

    debug(f"Поиск задачи по названию: '{find_title}'")
    task_id = find_task_id_by_title(webhook_url, find_title, project_id)
    
    if not task_id:
        if project_name:
            msg_text = f"Задача с названием, похожим на '{find_title}', не найдена в проекте '{project_name}'."
        else:
            msg_text = f"Задача с названием, похожим на '{find_title}', не найдена."
        msg = {"result": "error", "message": msg_text}
        debug(f"ОШИБКА: {msg['message']}")
        return json.dumps(msg, ensure_ascii=False)
    
    debug(f"Задача найдена. ID: {task_id}")

    fields_to_update = {}

    if "title" in args:
        fields_to_update["TITLE"] = args["title"]
    if "description" in args:
        fields_to_update["DESCRIPTION"] = args["description"]
    
    if "project" in args and project_id:
        fields_to_update["GROUP_ID"] = project_id

    if "responsible" in args:
        responsible_id = find_user_id_by_name(webhook_url, args["responsible"])
        if responsible_id:
            fields_to_update["RESPONSIBLE_ID"] = responsible_id
        else:
            debug(f"ПРЕДУПРЕЖДЕНИЕ: Ответственный '{args['responsible']}' не найден. Поле не будет обновлено.")

    if "deadline" in args:
        deadline = parse_deadline(args["deadline"])
        if deadline:
            fields_to_update["DEADLINE"] = deadline
        else:
            debug(f"ПРЕДУПРЕЖДЕНИЕ: Срок '{args['deadline']}' не распознан. Поле не будет обновлено.")

    if "status" in args:
        status_name = str(args["status"]).lower().strip()
        status_map = {
            "ждет выполнения": 2,
            "выполняется": 3,
            "ожидает контроля": 4, # Опечатка в старой версии, должно быть "Ждет контроля"
            "завершена": 5,
            "отложена": 6,
        }
        status_id = status_map.get(status_name)
        if status_id:
            fields_to_update["STATUS"] = status_id
            debug(f"Поле для обновления: Статус = {status_name} (ID: {status_id})")
        else:
            valid_statuses = ", ".join(status_map.keys())
            debug(f"ПРЕДУПРЕЖДЕНИЕ: Статус '{args['status']}' не распознан. Допустимые значения: {valid_statuses}. Поле не будет обновлено.")

    if "priority" in args:
        priority_arg = args["priority"]
        priority_map = { "высокий": "2", "средний": "1", "низкий": "0", "2": "2", "1": "1", "0": "0" }
        priority_value = priority_map.get(str(priority_arg).lower().strip())
        if priority_value:
            fields_to_update["PRIORITY"] = priority_value
            debug(f"Поле для обновления: Приоритет = {priority_arg} (ID: {priority_value})")
        else:
            debug(f"ПРЕДУПРЕЖДЕНИЕ: Приоритет '{priority_arg}' не распознан. Поле не будет обновлено.")

    if not fields_to_update:
        msg = {"result": "error", "message": "Не передано ни одного поля для обновления (title, description, project, responsible, deadline, status, priority)."}
        debug(f"ОШИБКА: {msg['message']}")
        return json.dumps(msg, ensure_ascii=False)

    debug(f"Итоговые поля для обновления задачи ID {task_id}: {fields_to_update}")

    task_result = update_b24_task(webhook_url, task_id, fields_to_update)
    
    if task_result:
        updated_task_id, task_link = task_result
        if updated_task_id and task_link:
            success_message = {"result": "success", "message": f"✅ Задача #{updated_task_id} успешно обновлена!\n\n🔗 Ссылка: {task_link}"}
            debug(f"Задача {updated_task_id} успешно обновлена.")
            return json.dumps(success_message, ensure_ascii=False)

    error_message = {"result": "error", "message": f"Произошла ошибка при обновлении задачи #{task_id} в Bitrix24."}
    debug(f"ОШИБКА: {error_message['message']}")
    return json.dumps(error_message, ensure_ascii=False)

# --- Точка входа для платформы NextBot ---
# Платформа выполняет этот файл и ожидает найти результат в переменной `result`.
result = main(args)
