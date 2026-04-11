[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/bRYoX1Iq)
# Python_async
Задание на использование асинхронности в Python

# Задание
**Медицинские диагностические устройства в клиниках.**
У медицинских клиник существуют чувствительные устройства, для которых важно плановое и своевременное обслуживание, для чего необходимо отслеживать статусы устройств.

**Необходимо асинхронно считать данные из файлов xlsx и произвести следующие действия асинхронно:**
* Отфильтровать данные по гарантии
* Найти клиники с наибольшим количеством проблем
* Построить отчёт по срокам калибровки
* Сагрегировать данные по клиникам и оборудованию и составить сводную таблицу

**Выполненные действия необходимо асинхронно записать в разные файлы.**

Поля в таблице:
* device_id – уникальный идентификатор устройства
* clinic_id – уникальный идентификатор клиники, где установлено или планируется установить устройство
* clinic_name – название клиники, в которой используется оборудование
* city – город, где установлен аппарат (всего 15 городов)
* department – медицинское отделение клиники, в котором используется оборудование
* model – модель устройства (всего 6 моделей)
* serial_number – серийный номер устройства
* install_date – дата установки оборудования в клинике (Если устройство ещё не установлено, дата может быть в будущем, форматы дат могут отличаться)
* status – текущий статус устройства (planned_installation – устройство запланировано к установке, operational – устройство работает, maintenance_scheduled – запланировано техническое обслуживание, faulty – устройство неисправно). В данных могут встречаться варианты написания (например OK, op, broken), которые нужно нормализовать.
* warranty_until – дата окончания гарантии производителя, после этой даты ремонт может выполняться на платной основе.
* last_calibration_date – дата последней калибровки оборудования (значение может отсутствовать, дата может быть ошибочной (раньше даты установки))
* last_service_date – дата последнего технического обслуживания (когда проводилось обслуживание, какие устройства требуют сервисной проверки)
* issues_reported_12mo – количество зарегистрированных проблем за последние 12 месяцев
* failure_count_12mo – количество отказов устройства за последние 12 месяцев
* uptime_pct – процент времени, в течение которого устройство было работоспособным
* issues_text – текстовое описание некоторых проблем, зарегистрированных в работе устройства

# Задача
- В качестве практической работы необходимо нарисовать блок-схемы работы алгоритма решения задач и загрузить в свой репозиторий в течение дня.
- В качестве домашней работы необходимо:
  * реализовать программы по ранее созданным алгоритмам решения задач на языке Python и загрузить в свой репозиторий до крайнего срока;
  * **Сравнить время выполнения** асинхронно выполненного кода и синхронно выполненного.

Даты сдачи оговариваются в канале группы.

# Асинхронность в Python

## Что такое асинхронность

**Асинхронность** — это способ организации программы, при котором она может **выполнять другие задачи, пока ждёт завершения медленных операций**.

 Ключевая идея: программа не простаивает во время ожидания.

---

## Синхронное выполнение

```python
import time

def task():
    time.sleep(2)
    print("Готово")

task()
task()
```

### Что происходит:
1. Первая задача выполняется 2 секунды
2. Затем вторая ещё 2 секунды

Итог: **4 секунды**

---

## Асинхронное выполнение

```python
import asyncio

async def task():
    await asyncio.sleep(2)
    print("Готово")

async def main():
    await asyncio.gather(task(), task())

asyncio.run(main())
```

* Обе задачи выполняются одновременно
* Итог: **2 секунды**

---

## Где возникает ожидание (I/O-bound задачи)

Асинхронность полезна, когда программа **ждёт**:

* HTTP-запросы
* Работа с файлами
* Базы данных
* Таймеры

---

## Важный момент

Асинхронность **не ускоряет код сама по себе**.

Она:

> Убирает время простоя (idle time)

---

## Аналогия

Повар:

### Синхронный:
* варит суп → ждёт
* потом жарит мясо

### Асинхронный:
* поставил суп
* пока варится → жарит мясо

---

## Основы asyncio

### 1. Корутины

```python
async def my_func():
    pass
```

### 2. await

```python
await some_operation()
```

Говорит: подожди результат, но не блокируй программу

### 3. Event Loop

Цикл событий управляет выполнением задач и переключением между ними

---

## Как это работает

```text
Задача A → ждёт → переключение на B
Задача B → ждёт → переключение на C
```

**Один поток, много задач**

---

## Асинхронность vs Потоки

| | Async | Threads |
|--|------|--------|
| Потоки | 1 | много |
| Переключение | через await | ОС |
| Нагрузка | низкая | выше |

---

## Когда использовать

### Подходит:
* HTTP-запросы
* Парсинг сайтов
* API
* Боты
* Чат-серверы

### Не подходит:
* Тяжёлые вычисления (CPU-bound)
* Простые скрипты

---

## Где не помогает

```python
for i in range(10**9):
    pass
```

**Это CPU-bound задача**

---

## Пример с HTTP

### Синхронно

```python
import requests

requests.get("https://example.com")
```

### Асинхронно

```python
import aiohttp
import asyncio

async def fetch():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://example.com") as r:
            return await r.text()

asyncio.run(fetch())
```

---

## Главное понимание

Асинхронность — это:

> Управление ожиданием

---

## Итог

Асинхронность нужна чтобы:

- не простаивать
- эффективно использовать ресурсы
- масштабировать I/O задачи

---

## 3 правила

1. Есть ожидание → используй async
2. Нет ожидания → async не нужен
3. Async = I/O, не CPU


---

# Event Loop

## Что такое Event Loop

Event Loop (цикл событий) — это ядро асинхронной программы.

Он управляет:
* выполнением задач (coroutines)
* переключением между ними
* обработкой событий (I/O, таймеры)

---

## Как он работает (упрощённо)

```text
1. Берёт задачу
2. Выполняет до await
3. Задача "засыпает"
4. Берёт следующую
```

В итоге:
* один поток
* много задач

---

## Пример переключения

```python
import asyncio

async def task1():
    print("A1")
    await asyncio.sleep(1)
    print("A2")

async def task2():
    print("B1")
    await asyncio.sleep(1)
    print("B2")

async def main():
    await asyncio.gather(task1(), task2())

asyncio.run(main())
```

Возможный вывод:
```
A1
B1
A2
B2
```

---

## Что происходит внутри

Когда ты пишешь:

```python
await asyncio.sleep(1)
```

Event Loop:
1. ставит задачу на паузу
2. регистрирует таймер
3. переключается на другую задачу
4. возвращается позже

---

## Основные компоненты

### 1. Task

Обёртка над coroutine:

```python
asyncio.create_task(coro())
```

позволяет запускать задачу независимо

---

### 2. Future

Объект, который хранит результат в будущем

используется внутри asyncio

---

### 3. Queue

Асинхронная очередь:

```python
queue = asyncio.Queue()
```

используется для pipeline и воркеров

---

## create_task vs await

```python
await coro()        # ждём завершения
asyncio.create_task(coro())  # запускаем параллельно
```

ключевое различие

---

## Частые ошибки

### Забыли await

```python
fetch()  # ничего не произойдёт
```

---

### Использование blocking-кода

```python
import time

time.sleep(1)  # блокирует всё
```

нужно:

```python
await asyncio.sleep(1)
```

---

### Смешивание sync и async

```python
requests.get(...)  # блокирует event loop
```

---

## Визуальная модель

```text
[Task A] → await → pause
[Task B] → await → pause
[Task C] → выполняется
```

Event Loop постоянно переключается

---

# Работа с HTTP: aiohttp

## Что такое aiohttp

`aiohttp` — это библиотека для выполнения HTTP-запросов асинхронно.

Используется вместе с `asyncio` для параллельной работы с сетью.

---

## Синхронный подход (requests)

```python
import requests

response = requests.get("https://example.com")
print(response.text)
```

Минус: блокирует выполнение программы

---

## Асинхронный подход (aiohttp)

```python
import aiohttp
import asyncio

async def fetch():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://example.com") as response:
            return await response.text()

async def main():
    html = await fetch()
    print(html[:100])

asyncio.run(main())
```

Здесь программа не блокируется во время ожидания ответа

---

## Параллельные запросы

```python
import aiohttp
import asyncio

urls = [
    "https://example.com",
    "https://example.org",
    "https://example.net",
]

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        results = await asyncio.gather(*tasks)

    print(len(results))

asyncio.run(main())
```

Все запросы выполняются одновременно

---

## Ограничение количества запросов (Semaphore)

```python
import asyncio
import aiohttp

sem = asyncio.Semaphore(5)

async def fetch(session, url):
    async with sem:
        async with session.get(url) as response:
            return await response.text()
```

Ограничивает количество одновременных запросов

---

## Обработка ошибок и retry

```python
async def fetch_with_retry(session, url, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                return await response.text()
        except Exception:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(1)
```

---

## Реальный пример: мини-парсер

```python
import aiohttp
import asyncio
from bs4 import BeautifulSoup

urls = [
    "https://example.com",
    "https://example.org",
]

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


def parse(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.title.text if soup.title else "No title"


async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        pages = await asyncio.gather(*tasks)

    results = [parse(html) for html in pages]
    print(results)

asyncio.run(main())
```

---

## Практические советы

- Используй один `ClientSession` на много запросов
- Всегда закрывай сессию через `async with`
- Добавляй `headers` (User-Agent)
- Ограничивай параллелизм
- Обрабатывай ошибки

---

## Итог

`aiohttp` позволяет:

- выполнять множество HTTP-запросов одновременно
- не блокировать программу
- эффективно работать с сетью

Это основной инструмент для асинхронного парсинга и работы с API
