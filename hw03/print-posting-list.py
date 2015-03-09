# encoding: utf-8
import sys
sys.path.append("../dfs/")

import client as dfs
import argparse
import json
import util
USERNAME="mylnikovorg"
metadata = dfs.CachedMetadata()

parser = argparse.ArgumentParser()
parser.add_argument("--term", required = True)
# args = parser.parse_args()
# args.term = "HANA"
ff="HANA"
# Ваш псевдоним в виде строковой константы
# USERNAME=
shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, util.encode_term(ff)[0:1]))])
plists = json.JSONDecoder().decode(shard)
print plists[util.encode_term(ff)]
