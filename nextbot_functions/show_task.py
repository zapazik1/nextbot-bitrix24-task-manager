STATUS_MAP = {
    "Новая": 1, # Это новый статус, который может быть по умолчанию
    "Ждет выполнения": 2,
    "Выполняется": 3,
    "Ждет контроля": 4,
    "Завершена": 5,
    "Отложена": 6,
}
REVERSE_STATUS_MAP = {v: k for k, v in STATUS_MAP.items()}

# Константы для доступа к Google Sheets (если они понадобятся)
# Я скопировал их из другого файла для согласованности.
# ...

def get_webhook_from_sheet(sheet_url, user_name):
    """
    Получает вебхук пользователя из опубликованной Google Таблицы CSV.
    Формат таблицы: A - Имя, B - Вебхук.
    """
    # В среде NextBot функция debug() доступна для логирования
    # debug(f"-> get_webhook_from_sheet: ищем вебхук для '{user_name}'")
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
                    # debug(f"<- get_webhook_from_sheet: Вебхук для '{user_name}' найден.")
                    if not webhook.endswith('/'): webhook += '/'
                    return webhook
        
        # debug(f"<- get_webhook_from_sheet: Пользователь '{user_name}' НЕ найден в таблице.")
        return None
    except Exception as e:
        # debug(f"<- get_webhook_from_sheet: Ошибка при доступе к Google Sheets: {e}")
        return None

def get_project_id(webhook, project_name):
    """
    Находит ID проекта (группы) в Битрикс24 по его названию.
    Использует нечеткий поиск по совпадению слов.
    """
    if not isinstance(project_name, str) or not project_name.strip():
        return None

    url = f"{webhook}sonet_group.get"
    try:
        response = requests.post(url, json={})
        response.raise_for_status()
        projects = response.json().get("result", [])
        if not projects:
            return None

        search_words = set(re.sub(r'[^\w\s]', '', project_name).lower().split())
        if not search_words:
            return None

        best_match_id = None
        max_common_count = 0

        for project in projects:
            current_project_name = project.get("NAME", "")
            project_words = set(re.sub(r'[^\w\s]', '', current_project_name).lower().split())
            common_count = len(search_words.intersection(project_words))

            if common_count > max_common_count:
                max_common_count = common_count
                best_match_id = int(project.get("ID"))

        return best_match_id if max_common_count > 0 else None
    except (requests.exceptions.RequestException, json.JSONDecodeError):
        return None

def get_user_name_by_id(webhook, user_id, user_cache):
    """
    Получает имя пользователя по его ID, используя кеширование.
    """
    if user_id in user_cache:
        return user_cache[user_id]

    if not user_id:
        return "Не назначен"

    url = f"{webhook}user.get.json"
    params = {'ID': user_id}
    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        result = response.json().get('result', [])
        if result:
            user = result[0]
            # Формируем полное имя
            name = user.get('NAME', '')
            last_name = user.get('LAST_NAME', '')
            full_name = f"{name} {last_name}".strip()
            user_cache[user_id] = full_name
            return full_name
        return f"ID: {user_id}"
    except (requests.exceptions.RequestException, json.JSONDecodeError):
        return f"ID: {user_id} (ошибка)"

def get_projects_map(webhook):
    """
    Получает все проекты (группы) и возвращает словарь {id: name}.
    """
    url = f"{webhook}sonet_group.get"
    params = {'ORDER': {'NAME': 'ASC'}}
    project_map = {0: "Личные (без проекта)"} # Для задач без проекта
    try:
        response = requests.post(url, json=params, timeout=10)
        response.raise_for_status()
        projects = response.json().get("result", [])
        for p in projects:
            project_id = int(p.get("ID"))
            project_name = p.get("NAME")
            if project_id and project_name:
                project_map[project_id] = project_name
        return project_map
    except (requests.exceptions.RequestException, json.JSONDecodeError, ValueError):
        # В случае ошибки вернем базовую карту, чтобы не ломать основной скрипт
        return project_map

def parse_deadline_for_filter(deadline_str):
    """
    Парсит строку с датой и возвращает словарь для фильтра Bitrix24.
    Поддерживает "сегодня", "завтра", "ДД.ММ.ГГГГ".
    Возвращает словарь с ключами '>=DEADLINE' и '<=DEADLINE'.
    """
    if not deadline_str:
        return {}

    deadline_str = deadline_str.lower().strip()
    today = datetime.datetime.now()
    filter_date = None

    if "сегодня" in deadline_str:
        filter_date = today
    elif "завтра" in deadline_str:
        filter_date = today + datetime.timedelta(days=1)
    else:
        try:
            # Ручной парсинг "ДД.ММ.ГГГГ"
            parts = deadline_str.split('.')
            day = int(parts[0])
            month = int(parts[1])
            year = int(parts[2])
            filter_date = datetime.datetime(year=year, month=month, day=day)
        except (ValueError, IndexError):
            return {} # Неверный формат, возвращаем пустой фильтр

    if filter_date:
        start_of_day = filter_date.replace(hour=0, minute=0, second=0)
        end_of_day = filter_date.replace(hour=23, minute=59, second=59)
        
        # Ручное форматирование даты в строку
        start_str = f"{start_of_day.year:04d}-{start_of_day.month:02d}-{start_of_day.day:02d} {start_of_day.hour:02d}:{start_of_day.minute:02d}:{start_of_day.second:02d}"
        end_str = f"{end_of_day.year:04d}-{end_of_day.month:02d}-{end_of_day.day:02d} {end_of_day.hour:02d}:{end_of_day.minute:02d}:{end_of_day.second:02d}"
        
        return {
            '>=DEADLINE': start_str,
            '<=DEADLINE': end_str,
        }

    return {}

def main(args):
    """
    Основная функция для получения и форматирования списка задач.
    """
    # 1. Получение вебхука
    user_name = args.get("nameUser")
    webhook = args.get("webhook")
    if not webhook:
        gsheet_url = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/pub?gid=0&single=true&output=csv"
        if not user_name:
            error_message = {"status": "error", "message": "Ошибка: Не удалось определить пользователя для поиска вебхука."}
            return json.dumps(error_message, ensure_ascii=False)
        webhook = get_webhook_from_sheet(gsheet_url, user_name)

    if not webhook:
        error_message = {"status": "error", "message": f"Ошибка: Вебхук для пользователя '{user_name}' не найден."}
        return json.dumps(error_message, ensure_ascii=False)
    if not webhook.endswith('/'):
        webhook += '/'

    # 2. Формирование фильтра
    task_filter = {'!STATUS': '5'}  # Исключаем завершенные задачи

    project_name_arg = args.get('project_name')
    if project_name_arg:
        project_id = get_project_id(webhook, project_name_arg)
        if project_id:
            task_filter['GROUP_ID'] = project_id
        else:
            error_message = {"status": "error", "message": f"Проект с названием '{project_name_arg}' не найден."}
            return json.dumps(error_message, ensure_ascii=False)

    deadline_str = args.get('deadline')
    if deadline_str:
        deadline_filter = parse_deadline_for_filter(deadline_str)
        if deadline_filter:
            task_filter.update(deadline_filter)
        else:
            error_message = {"status": "error", "message": f"Не удалось распознать формат крайнего срока: '{deadline_str}'. Используйте 'сегодня', 'завтра' или 'ДД.ММ.ГГГГ'."}
            return json.dumps(error_message, ensure_ascii=False)

    # 3. Выполнение запроса к API
    select_fields = ["ID", "TITLE", "DESCRIPTION", "DEADLINE", "STATUS", "RESPONSIBLE_ID", "GROUP_ID"]
    params = {
        'order': {'ID': 'DESC'},
        'filter': task_filter,
        'select': select_fields
    }
    
    try:
        response = requests.post(webhook + 'tasks.task.list', json=params, timeout=15)
        response.raise_for_status()
        result = response.json()
        
        if 'error' in result and result['error']:
            error_message = {"status": "error", "message": f"Ошибка API Bitrix24: {result.get('error_description', 'Нет описания')}"}
            return json.dumps(error_message, ensure_ascii=False)

        tasks = result.get('result', {}).get('tasks', [])
        
        if not tasks:
            success_message = {"status": "success", "projects": [], "message": "Задачи по вашим критериям не найдены."}
            return json.dumps(success_message, ensure_ascii=False)

        # 4. Группировка задач по проектам
        project_map = None
        if not project_name_arg: # Если проект не был задан, получаем карту всех проектов
            project_map = get_projects_map(webhook)
        
        grouped_tasks = {}
        user_cache = {}
        for task in tasks:
            title = task.get('title', 'Без названия')
            description = task.get('description', 'Без описания')
            description = re.sub(r'\[DISK FILE ID=[^\]]+\]', '', description).strip()

            deadline = task.get('deadline')
            if deadline:
                try:
                    date_part = deadline.split('T')[0]
                    time_part = deadline.split('T')[1].split('+')[0]
                    date_parts = date_part.split('-')
                    year = date_parts[0]
                    month = date_parts[1]
                    day = date_parts[2]
                    time_parts = time_part.split(':')
                    hour = time_parts[0]
                    minute = time_parts[1]
                    deadline_formatted = f"{day}.{month}.{year} {hour}:{minute}"
                except (ValueError, IndexError):
                    deadline_formatted = "Неверный формат даты"
            else:
                deadline_formatted = "Не указан"
                
            status_id = int(task.get('status', 0))
            status_text = REVERSE_STATUS_MAP.get(status_id, f"Неизвестный статус ({status_id})")
            
            responsible_id = int(task.get('responsibleId', 0))
            responsible_name = get_user_name_by_id(webhook, responsible_id, user_cache)
            
            task_data = {
                "title": title,
                "description": description or 'Нет',
                "deadline": deadline_formatted,
                "status": status_text,
                "responsible": responsible_name
            }
            
            task_project_name = ""
            if project_name_arg:
                task_project_name = project_name_arg
            elif project_map:
                group_id = int(task.get('groupId', 0))
                task_project_name = project_map.get(group_id, f"Проект ID:{group_id}")
            else: # Запасной вариант, если карта проектов не загрузилась
                group_id = int(task.get('groupId', 0))
                task_project_name = f"Проект ID:{group_id}" if group_id != 0 else "Личные (без проекта)"

            if task_project_name not in grouped_tasks:
                grouped_tasks[task_project_name] = []
            grouped_tasks[task_project_name].append(task_data)

        # 5. Форматирование итогового JSON
        projects_output = []
        sorted_project_names = sorted(grouped_tasks.keys())
        for p_name in sorted_project_names:
            projects_output.append({
                "projectName": p_name,
                "tasks": grouped_tasks[p_name]
            })
            
        final_result = {"status": "success", "projects": projects_output}
        return json.dumps(final_result, ensure_ascii=False)

    except requests.exceptions.RequestException as e:
        error_message = {"status": "error", "message": f"Ошибка сети при обращении к Bitrix24: {e}"}
        return json.dumps(error_message, ensure_ascii=False)
    except (json.JSONDecodeError, KeyError) as e:
        error_message = {"status": "error", "message": f"Ошибка обработки ответа от Bitrix24: {e}"}
        return json.dumps(error_message, ensure_ascii=False)


# Точка входа для платформы NextBot
# Ожидаемая платформой NextBot строка
result = main(args)
