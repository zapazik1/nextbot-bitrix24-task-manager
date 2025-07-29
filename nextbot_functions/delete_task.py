

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
            if not line.strip():
                continue
                
            parts = line.strip().split(',')
            if len(parts) >= 2:
                sheet_user = parts[0].strip()
                webhook = parts[1].strip()
                webhook = ''.join(c for c in webhook if c.isprintable())
                
                if sheet_user == user_name:
                    debug(f"<- get_webhook_from_sheet: Вебхук для '{user_name}' найден.")
                    if not webhook.endswith('/'):
                        webhook += '/'
                    return webhook
        
        debug(f"<- get_webhook_from_sheet: Пользователь '{user_name}' НЕ найден в таблице.")
        return None

    except requests.exceptions.RequestException as e:
        debug(f"<- get_webhook_from_sheet: Ошибка при запросе к Google Sheets: {e}")
        return None
    except Exception as e:
        debug(f"<- get_webhook_from_sheet: Непредвиденная ошибка: {e}")
        return None

def find_task_id_by_title(webhook_url: str, title: str, project_id: int or None = None) -> int or None:
    """
    Ищет ID задачи в Bitrix24 по наиболее похожему названию.
    Если указан project_id, ищет только в этом проекте.
    """
    debug(f"-> find_task_id_by_title (fuzzy): '{title}', project_id: {project_id}")
    url = f"{webhook_url}tasks.task.list.json"
    
    # Формируем фильтр
    task_filter = {"ZOMBIE": "N"}
    if project_id is not None:
        task_filter["GROUP_ID"] = project_id
        
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

def find_project_id_by_name(webhook_url: str, project_name: str) -> int or None:
    """Ищет ID проекта (рабочей группы) в Bitrix24 по наиболее похожему названию (метод 'мешка слов')."""
    debug(f"-> find_project_id_by_name (fuzzy): '{project_name}'")
    url = f"{webhook_url}sonet_group.get.json"
    params = {} 

    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        projects = response.json().get("result", [])

        if not projects:
            debug("<- find_project_id_by_name (fuzzy): Список проектов пуст.")
            return None
            
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

        if best_match_id and max_common_count > 0:
            debug(f"<- find_project_id_by_name (fuzzy): Найден наиболее похожий проект ID: {best_match_id}")
            return best_match_id
        else:
            debug(f"<- find_project_id_by_name (fuzzy): Не найдено достаточно похожего проекта для '{project_name}'.")
            return None

    except requests.exceptions.RequestException as e:
        debug(f"<- find_project_id_by_name (fuzzy): ОШИБКА API: {e}")
    return None

def delete_b24_task(webhook_url: str, task_id: int) -> bool:
    """Удаляет задачу в Bitrix24 по ее ID."""
    debug(f"-> delete_b24_task: ID={task_id}")
    url = f"{webhook_url}tasks.task.delete.json"
    params = {"taskId": task_id}
    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        result_json = response.json()
        
        # Метод delete возвращает {"result": true} в случае успеха
        if result_json.get("result") is True:
            debug(f"<- delete_b24_task: Задача ID {task_id} успешно удалена.")
            return True
        else:
            debug(f"<- delete_b24_task: API вернуло ошибку при удалении задачи {task_id}. Ответ: {result_json}")
            return False
            
    except requests.exceptions.RequestException as e:
        debug(f"<- delete_b24_task: ОШИБКА API: {e}")
        if 'response' in locals() and hasattr(response, 'text'):
            debug(f"Ответ от сервера: {response.text}")
    except Exception as e:
        debug(f"<- delete_b24_task: Непредвиденная ошибка: {e}")
    
    debug(f"<- delete_b24_task: Не удалось удалить задачу {task_id}.")
    return False

# --- Основная функция, которую вызывает платформа ---

def main(args: dict) -> dict:
    """
    Основная логика удаления задачи в Bitrix24.
    Ищет задачу по 'title' и удаляет ее.
    """
    debug("--- Запуск функции delete_task ---")
    debug(f"Получены аргументы от NextBot: {args}")

    GSHEET_URL = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/pub?gid=0&single=true&output=csv"
    user_name = args.get("nameUser")
    if not user_name:
        return {"result": "error", "message": "Техническая ошибка: не было передано имя пользователя (nameUser)."}

    webhook_url = get_webhook_from_sheet(GSHEET_URL, user_name)
    if not webhook_url:
        msg = f"Не удалось найти вебхук для пользователя '{user_name}'. Убедитесь, что вы внесены в базу."
        return {"result": "error", "message": msg}
    
    title_to_delete = args.get("title")
    if not title_to_delete:
        return {"result": "error", "message": "Необходимо указать 'title' для поиска и удаления задачи."}

    project_name = args.get("project_name")
    project_id = None
    if project_name:
        project_id = find_project_id_by_name(webhook_url, project_name)
        if not project_id:
            msg = f"Проект с названием, похожим на '{project_name}', не найден. Удаление отменено."
            return {"result": "error", "message": msg}

    task_id = find_task_id_by_title(webhook_url, title_to_delete, project_id)
    if not task_id:
        if project_name:
            msg = f"Задача с названием, похожим на '{title_to_delete}', не найдена в проекте '{project_name}'."
        else:
            msg = f"Задача с названием, похожим на '{title_to_delete}', не найдена."
        return {"result": "error", "message": msg}
    
    was_deleted = delete_b24_task(webhook_url, task_id)

    if was_deleted:
        success_message = f"✅ Задача #{task_id} ('{title_to_delete}') успешно удалена."
        return {"result": "success", "message": success_message}
    else:
        return {"result": "error", "message": f"Произошла ошибка при удалении задачи #{task_id} в Bitrix24."}

# --- Точка входа для платформы NextBot ---
# Платформа выполняет этот файл и ожидает найти результат в переменной `result`.
result = main(args)
