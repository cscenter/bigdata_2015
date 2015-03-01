# encoding: utf-8
from __future__ import print_function

import argparse
from dfs_client import *


def print_matrix_chunk(filename, cols):
  row = 0
  col = 0
  for l in get_file_content(filename):
    if row == 0:
      matrix_num, start, end = l.split(" ", 2)
      print("Matrix #%s Rows [%s, %s]" % (matrix_num, start, end))
      row += 1
      continue
    
    print(l, end=' ')
    col += len(l.split(" "))
    if col == cols:
      print("")
      row += 1
      col = 0
  return row - 1

def print_matrix(matrix_toc, rows, cols):
  print("Printing matrix %dx%d" % (rows, cols))
  read_rows = 0
  for l in get_file_content(matrix_toc):
    read_rows += print_matrix_chunk(l, cols)
  if rows != read_rows:
    print("Что-то пошло не так: мы прочитали %d строк а надо было %d" % (read_rows, rows))

parser = argparse.ArgumentParser()
parser.add_argument("--num", required = True, help = "Номер матрицы")
parser.add_argument("--rows", required = True, help = "Число строк")
parser.add_argument("--cols", required = True, help = "Число столбцов")

args = parser.parse_args()
print_matrix("/matrix%s" % args.num, int(args.rows), int(args.cols))
