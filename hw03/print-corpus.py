import sys
sys.path.append("../dfs/")

import client as dfs
import argparse

metadata = dfs.CachedMetadata()

def print_file(filename):
	for l1 in metadata.get_file_content(filename):
		print l1
	print "\n\n"


parser = argparse.ArgumentParser()
parser.add_argument("--file", required = False, default = None)
args = parser.parse_args()

if args.file is None:
	for l in metadata.get_file_content("/wikipedia/__toc__"):
		filename, pagename = l.split(" ", 1)
		print pagename
		print '==========================='
		print_file(filename)
else:
	print_file(args.file)