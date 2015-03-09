m = [1,2,3]
n = [1,4,9]
new_tuple = sum(map(lambda x, y: x*y, m, n))
print(new_tuple)
# new_tuple [(1, 1), (2, 4), (3, 9)]