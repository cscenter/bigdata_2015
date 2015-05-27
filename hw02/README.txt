 
Добавленны файлы mr_mult_matrix_step1.py и mr_mult_matrix_step2.py
Выполняющие перемножение матриц в два этапа map-reduce. так как мне не удалось выяснить как в mincemeat
задать последовательность reduce-ов, то результат первого шага записывается в файловую систему и читается 
при выполнении второго шага.

Схема запуска аналогична mr_sum_matrix.py:

Запуск диспетчера map-reduce: python mr_mult_matrix_step1.py
Запуск рабочего процесса: python mincemeat.py localhost

после выполнения:

Запуск диспетчера map-reduce: python mr_mult_matrix_step2.py
Запуск рабочего процесса: python mincemeat.py localhost

результат работы программы будет записан в локальный файл result.txt

данная реализация создает линейное (по максимальному из размеров матриц) количество ключей










В скрипте print_matrix.py реализована программа, печатающая строки матрицы MxN,
хранящиеся в test_dfs описанным в задании образом. У нас есть две матрицы: первая размером
3x4, состоящая из единиц и вторая размером 4x6, состоящая из двоек. Запуск программы:

python print_matrix.py --num 1 --rows 3 --cols 4
python print_matrix.py --num 2 --rows 4 --cols 6

В файле mr_sum_matrix.py реализован map-reduce, считающий для каждой матрицы сумму ее элементов.
Запуск диспетчера map-reduce: 
python mr_sum_matrix.py

Запуск рабочего процесса: python mincemeat.py localhost

print_matrix.py работает как в Python 2 так и в Python 3
mr_sum_matrix.py и mincemeat рассчитаны на Python 2

Обратите внимание на то, что если вы будете в ваших функциях mapfn и reducefn использовать
сторонние модули, вам, скорее всего, понадобится эти модули включить в mincemeat.py. 
Так сделано, например, с dfs_client.
Status API Training Shop Blog About
© 2015 GitHub, Inc. Terms Privacy Security Contact
