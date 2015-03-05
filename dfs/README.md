### TL;DR:
```
cd bigdata_2015/dfs
rm -r files data data2 
mkdir data data2
python server.py --role master --port 8000
python server.py --role chunkserver --master localhost:8000 --chunkserver localhost --port 8001 --data data
python server.py --role chunkserver --master localhost:8000 --chunkserver localhost --port 8002 --data data2
```

### Подробности
DFS состоит из нескольких серверов: мастера и как минимум одного файлового сервера. Мастер хранит метаданные, файловые серверы хранят фрагменты в каталоге на диске. Метаданные включают в себя наш знакомый файл `files` -- отоюражение имен файлов в идентификаторы фрагментов. Информацию о расположении фрагментов (известную нам как `chunk_locations`) мастер получает от файловых серверов.

В командах выше запускаются с чистого листа 1 мастер и 2 файловых сервера. Мастер запускается на порту 8000, файловые серверы запускаются на портах 8001 и 8002 и хранят данные, соответственно, в каталогах `data` и `data2`. 

### Клиентский код
В скрипте `client.py` есть уже знакомые функции `files()`, `chunk_locations()`, `get_chunk_data()`, `get_file_content()` -- они делают то же самое что и делали в предыдущих заданиях. Новые функции `create_chunk()` и `write_chunk_data()` создают новый фрагмент и записывают его содержимое. Вспомогательная функция `file_appender()` возвращает объект с методом `write()` который буферизует записи и при закрытии создаёт новый фрагмент и записывает в него накопленные записи. Этим объектом удобно пользоваться таким образом:

```
with dfs.file_appender("/foo/bar") as f:
    f.write("Foo")
    f.write("Bar")
# Здесь мы выйдем из контекста with, автоматически вызовется метод f.close() 
# и в файле /foo/bar создастся новый фрагмент. 
# Если файла не существовало, он появится
```

Клиентский код по умолчанию обращается к мастеру, висящему на `localhost:8000` (константа `MASTER_URL` в `client.py`)

### Необходимые модули
Для работы DFS требуется Python 2 и модуль `poster`. Модуль `poster` отсутствует в стандартном SDK, ставится командой pip install poster (возможно, запущенной от имени суперпользователя)

### Подводные камни
```
python server.py --role chunkserver --master localhost:8000 --chunkserver localhost --port 8001 --data data
Traceback (most recent call last):
  File "server.py", line 309, in <module>
    send_heartbeat()
  File "server.py", line 153, in send_heartbeat
    self.send_error(404, "No permission to list directory")
NameError: global name 'self' is not defined
```

Сделайте каталог `data`

```
  File "print-corpus.py", line 20, in <module>
    for l in metadata.get_file_content("/wikipedia/__toc__"):
  File "../dfs/client.py", line 99, in get_file_content
    for l in get_chunk_data(self.chunk_locations[chunk_id], chunk_id):
KeyError: u'6bf8debcb30e532944848c2329315072_0'
```

Если в `chunk_locations` не находится какой-то ключ, который есть в `files`, это означает, что файловый сервер, хранящий соответствующий фрагмент, не запущен или у него неправильно указан аргумент `--data` 
### 
