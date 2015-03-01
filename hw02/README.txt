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
