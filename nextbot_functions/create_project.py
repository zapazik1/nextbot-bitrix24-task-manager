
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

def get_current_user_id(webhook_url):
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

def find_user_ids_by_names(webhook_url, names):
    """Находит ID пользователей по их именам."""
    debug(f"-> find_user_ids_by_names: ищем пользователей: {names}")
    if not names:
        return []
    
    url = f"{webhook_url}user.get.json"
    try:
        response = requests.post(url, timeout=5)
        response.raise_for_status()
        users = response.json().get("result", [])
        
        found_ids = []
        for name in names:
            name_lower = name.lower()
            for user in users:
                if (name_lower in user.get("NAME", "").lower() or 
                    name_lower in user.get("LAST_NAME", "").lower() or 
                    name_lower in user.get("SECOND_NAME", "").lower()):
                    found_ids.append(int(user["ID"]))
                    break
        
        debug(f"<- find_user_ids_by_names: найдены ID: {found_ids}")
        return found_ids
    except Exception as e:
        debug(f"<- find_user_ids_by_names: Ошибка при поиске пользователей: {e}")
        return []

def create_b24_project(webhook_url, fields):
    """Создает проект в Bitrix24 и возвращает его ID и ссылку."""
    debug(f"-> create_b24_project: с полями {fields}")
    url = f"{webhook_url}sonet_group.create.json"
    params = {"fields": fields}
    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        result_json = response.json()
        if "result" in result_json:
            project_id = result_json["result"]
            if project_id:
                portal_url = webhook_url.split('/rest/')[0]
                project_link = f"{portal_url}/workgroups/group/{project_id}/"
                debug(f"<- create_b24_project: проект создан, ID: {project_id}, ссылка: {project_link}")
                return project_id, project_link
    except Exception as e:
        debug(f"<- create_b24_project: ОШИБКА API: {e}")
        debug(f"Ответ от сервера: {response.text}")
    
    debug("<- create_b24_project: не удалось создать проект, возвращает None, None")
    return None, None

def main(args):
    """Основная функция для создания проекта."""
    debug("--- Запуск функции create_project ---")
    debug(f"Получены аргументы от NextBot: {args}")

    GSHEET_URL = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/pub?gid=0&single=true&output=csv"
    user_name = args.get("nameUser")
    if not user_name:
        return {"result": "error", "message": "Техническая ошибка: не было передано имя пользователя (nameUser)."}

    webhook_url = get_webhook_from_sheet(GSHEET_URL, user_name)
    if not webhook_url:
        msg = f"Не удалось найти вебхук для пользователя '{user_name}'. Убедитесь, что вы внесены в базу."
        return {"result": "error", "message": msg}

    project_name = args.get("name")
    directors = args.get("directors", [])
    team = args.get("team", [])

    if not project_name:
        return {"result": "error", "message": "Необходимо указать название проекта."}

    # Получаем ID текущего пользователя как руководителя по умолчанию
    current_user_id = get_current_user_id(webhook_url)
    if not current_user_id:
        return {"result": "error", "message": "Не удалось определить текущего пользователя."}

    # Формируем список руководителей
    director_ids = []
    if directors:
        director_ids = find_user_ids_by_names(webhook_url, directors)
        if not director_ids:
            return {"result": "error", "message": "Не удалось найти указанных руководителей."}
    else:
        director_ids = [current_user_id]

    # Формируем список участников команды
    team_ids = []
    if team:
        team_ids = find_user_ids_by_names(webhook_url, team)
        if not team_ids:
            return {"result": "error", "message": "Не удалось найти указанных участников команды."}

    # Создаем проект
    fields = {
        "NAME": project_name,
        "VISIBLE": "Y",
        "OPENED": "Y",
        "CLOSED": "N",
        "SUBJECT_ID": 1,  # Стандартная категория
        "KEYWORDS": "",
        "DESCRIPTION": "",
        "PROJECT": "Y",
        "IS_EXTRANET": "N",
        "OWNER_ID": current_user_id,
        "MEMBERS": director_ids + team_ids
    }

    project_result = create_b24_project(webhook_url, fields)
    project_id = project_result[0]
    project_link = project_result[1]
    
    if project_id and project_link:
        success_message = f"✅ Проект «{project_name}» успешно создан!\\n\\n🔗 Ссылка: {project_link}"
        return {"result": "success", "message": success_message}
    else:
        return {"result": "error", "message": "Произошла ошибка при создании проекта в Bitrix24."}

# --- Точка входа для платформы NextBot ---
# Платформа выполняет этот файл и ожидает найти результат в переменной `result`.
result = main(args)
