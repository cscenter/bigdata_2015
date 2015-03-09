# encoding: utf-8
import sys
sys.path.append("../dfs/")

import client as dfs
import argparse
import json
import util

metadata = dfs.CachedMetadata()

parser = argparse.ArgumentParser()
parser.add_argument("--term", required = True)
args = parser.parse_args()

# Ваш псевдоним в виде строковой константы
# USERNAME=
shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % ('khazhoyan', util.encode_term(args.term)[0:1]))])
plists = json.JSONDecoder().decode(shard)
print plists[util.encode_term(args.term)]
