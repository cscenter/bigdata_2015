Запуск мастера DFS:
python server.py --role master --port 8000

Запуск файлсервера, хранящего файлы в каталоге data:
python server.py --role chunkserver --master localhost:8000 --chunkserver localhost --port 8001 --data data

Запуск второго файлсервера, хранящего файлы в каталоге data2:
python server.py --role chunkserver --master localhost:8000 --chunkserver localhost --port 8002 --data data2