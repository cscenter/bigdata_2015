#!/usr/bin/env python
# -*- coding: utf-8 -*-

for i in xrange(9):
    for j in xrange(8):
        v = 0
        for k in xrange(6):
            with open('target/temp_result_%d_%d_%d.dat' % (i, j, k), 'r') as f:
                v += int(f.read().rstrip())
        # with open('res_%d_%d.txt' % (i, j), 'r') as f:
        #     v += int(f.read().rstrip())
        print v,
    print
