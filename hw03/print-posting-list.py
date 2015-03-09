# encoding: utf-8
from __future__ import print_function
import argparse
import json
import util
import sys
sys.path.append('../dfs/')
import client as dfs

metadata = dfs.CachedMetadata()

parser = argparse.ArgumentParser()
parser.add_argument('--term', required=True)
args = parser.parse_args()

# Ваш псевдоним в виде строковой константы
USERNAME = 'nightuser'
shard = ''.join(list(metadata.get_file_content('/%s/posting_list/%s' %
                     (USERNAME, util.encode_term(args.term)[0]))))
plists = json.loads(shard)
print(plists[util.encode_term(args.term)])
