

def get_webhook_from_sheet(sheet_url: str, user_name: str) -> str or None:
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

def parse_deadline(deadline_str: str) -> str or None:
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
            value = int(match.group(1))
            unit = match.group(2)
            if "дн" in unit:
                deadline_dt = (now + datetime.timedelta(days=value)).replace(hour=18, minute=0, second=0)
            elif "час" in unit:
                deadline_dt = now + datetime.timedelta(hours=value)
    else:
        try:
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

def find_project_id_by_name(webhook_url: str, project_name: str) -> int or None:
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

def find_user_id_by_name(webhook_url: str, user_name: str) -> int or None:
    """Ищет ID пользователя в Bitrix24 по имени, фамилии или частичному совпадению."""
    debug(f"-> find_user_id_by_name: '{user_name}'")
    url = f"{webhook_url}user.search.json"
    params = {"FILTER": {"FIND": user_name}}
    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        result_json = response.json()
        if result_json.get("result") and len(result_json["result"]) > 0:
            user_id = result_json["result"][0].get("ID")
            debug(f"<- find_user_id_by_name: пользователь найден, ID: {user_id}")
            return user_id
        else:
            debug(f"<- find_user_id_by_name: пользователь '{user_name}' не найден.")
            return None
    except requests.exceptions.RequestException as e:
        debug(f"<- find_user_id_by_name: ОШИБКА API: {e}")
    return None

def create_b24_task(webhook_url: str, fields: dict) -> (int or None, str or None):
    """Создает задачу в Bitrix24 и возвращает ее ID и ссылку."""
    debug(f"-> create_b24_task: с полями {fields}")
    url = f"{webhook_url}tasks.task.add.json"
    params = {"fields": fields}
    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        result_json = response.json()
        if "result" in result_json and "task" in result_json["result"]:
            task = result_json["result"]["task"]
            task_id = task.get("id")
            if task_id:
                portal_url = webhook_url.split('/rest/')[0]
                creator_id = task.get("createdBy")
                task_link = f"{portal_url}/company/personal/user/{creator_id}/tasks/task/view/{task_id}/"
                debug(f"<- create_b24_task: задача создана, ID: {task_id}, ссылка: {task_link}")
                return task_id, task_link
    except requests.exceptions.RequestException as e:
        debug(f"<- create_b24_task: ОШИБКА API: {e}")
        debug(f"Ответ от сервера: {response.text}")
    
    debug("<- create_b24_task: не удалось создать задачу, возвращает None, None")
    return None, None

def get_current_user_id(webhook_url: str) -> int or None:
    """Получает ID пользователя, которому принадлежит вебхук."""
    debug("-> get_current_user_id: запрашиваем данные текущего пользователя")
    url = f"{webhook_url}user.current.json"
    try:
        response = requests.post(url, timeout=5)
        response.raise_for_status()
        result = response.json().get("result", {})
        user_id = result.get("ID")
        if user_id:
            debug(f"<- get_current_user_id: ID текущего пользователя: {user_id}")
            return int(user_id)
        else:
            debug("<- get_current_user_id: Не удалось получить ID из ответа.")
            return None
    except Exception as e:
        debug(f"<- get_current_user_id: Ошибка при получении данных пользователя: {e}")
        return None

# --- Основная функция, которую вызывает платформа ---

def main(args: dict) -> dict:
    debug("--- Запуск функции add_new_task ---")
    debug(f"Получены аргументы от NextBot: {args}")

    GSHEET_URL = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/pub?gid=0&single=true&output=csv"
    user_name = args.get("nameUser")
    if not user_name:
        return {"result": "error", "message": "Техническая ошибка: не было передано имя пользователя (nameUser)."}

    webhook_url = get_webhook_from_sheet(GSHEET_URL, user_name)

    if not webhook_url:
        msg = f"Не удалось найти вебхук для пользователя '{user_name}'. Убедитесь, что вы внесены в базу."
        return {"result": "error", "message": msg}

    task_title = args.get("title")
    task_description = args.get("description", "")
    project_name = args.get("project")
    responsible_name = args.get("responsible")
    deadline_str = args.get("deadline")

    if not task_title:
        return {"result": "error", "message": "Необходимо указать название задачи."}

    project_id = None
    if project_name:
        project_id = find_project_id_by_name(webhook_url, project_name)
        if not project_id:
            msg = f"Проект, похожий на '{project_name}', не найден. Задача не была создана."
            return {"result": "error", "message": msg}

    responsible_id = None
    if responsible_name:
        responsible_id = find_user_id_by_name(webhook_url, responsible_name)
        if not responsible_id:
            return {"result": "error", "message": f"Пользователь '{responsible_name}' не найден. Проверьте имя."}
        debug(f"Ответственный найден по имени. ID: {responsible_id}")
    else:
        # Ответственный не указан, используем владельца вебхука
        responsible_id = get_current_user_id(webhook_url)
        if not responsible_id:
            # В качестве запасного варианта, если API не ответил, ставим администратора (ID=1)
            responsible_id = 1
            debug("Не удалось определить владельца вебхука, используется ID по умолчанию: 1")
        else:
            debug(f"Ответственный не указан. Используется владелец вебхука. ID: {responsible_id}")

    deadline = None
    if deadline_str:
        deadline = parse_deadline(deadline_str)

    fields = {
        "TITLE": task_title,
        "DESCRIPTION": task_description,
        "RESPONSIBLE_ID": responsible_id,
    }
    if project_id: fields["GROUP_ID"] = project_id
    if deadline: fields["DEADLINE"] = deadline

    priority_arg = args.get("priority", "1")
    priority_map = {"высокий": "2", "средний": "1", "низкий": "0", "2": "2", "1": "1", "0": "0"}
    fields["PRIORITY"] = priority_map.get(str(priority_arg).lower().strip(), "1")

    task_result = create_b24_task(webhook_url, fields)
    task_id = task_result[0]
    task_link = task_result[1]
    
    if task_id and task_link:
        success_message = f"✅ Задача «{task_title}» успешно создана!\\n\\n🔗 Ссылка: {task_link}"
        return {"result": "success", "message": success_message}
    else:
        return {"result": "error", "message": "Произошла ошибка при создании задачи в Bitrix24."}

# --- Точка входа для платформы NextBot ---
# Платформа выполняет этот файл и ожидает найти результат в переменной `result`.
result = main(args)
