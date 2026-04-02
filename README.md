# Telnyx SMS Panel

Веб-панель для отправки и получения SMS через Telnyx.

## Быстрый старт (Railway - бесплатно)

### 1. Деплой на Railway

1. Зайди на https://railway.app и войди через GitHub
2. New Project -> Deploy from GitHub repo
3. Залей эту папку в новый репозиторий на GitHub, подключи его
4. В Railway добавь переменные окружения (Settings -> Variables):
   - `TELNYX_API_KEY` = твой API ключ из Telnyx Portal
   - `MY_NUMBER` = твой номер в Telnyx, например `+15512300914`
5. Railway автоматически даст тебе URL вида `https://xxx.railway.app`

### 2. Настройка Webhook в Telnyx

1. Зайди в Telnyx Portal -> Messaging -> Phone Numbers
2. Нажми на свой номер
3. В поле "Webhook URL" укажи:
   ```
   https://ВАШ_URL.railway.app/webhook/telnyx
   ```
4. Сохрани

### 3. Готово!

- Открой `https://ВАШ_URL.railway.app` в браузере
- Входящие SMS будут появляться автоматически
- Нажми "+" чтобы написать новому контакту

---

## Локальный запуск

```bash
pip install -r requirements.txt
export TELNYX_API_KEY=your_key
export MY_NUMBER=+15512300914
python app.py
```

Для получения входящих локально используй ngrok:
```bash
ngrok http 5000
# Webhook URL: https://xxxx.ngrok.io/webhook/telnyx
```

## Структура проекта

```
app.py          - Flask сервер, API, webhook обработчик
templates/
  index.html    - Веб-интерфейс панели
requirements.txt
Procfile        - Для Railway/Render
```
