#!/bin/bash

# M1(m, k) * M2(k, n)

# Первый mapreduce создает пары key = (строка, столбец, i)
# value = M1(стока, i) * M2(i, столбец)
python mr_mult_matrix1.py &
sleep 0.1 ;
python mincemeat.py localhost ;

# Воторой mapreduce суммирует пары из первый части
python mr_mult_matrix2.py &
sleep 0.1 ;
python mincemeat.py localhost ;

# Требется много памяти. Если сначало матрицы занимают m * k + k * n,
# то после первого mr потребуется m * k * n памяти. То есть нужно
# использовать DFS для хранения промежуточных данных результата.



