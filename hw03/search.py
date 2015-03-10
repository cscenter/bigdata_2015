# encoding: utf-8
import sys

sys.path.append("../dfs/")
import client as dfs
import argparse
import json
import util
from collections import Counter
from math import sqrt

USERNAME = 'khazhoyan'

corpus_size = [float(l) for l in dfs.get_file_content('/wikipedia/__size__')][0]

metadata = dfs.CachedMetadata()
parser = argparse.ArgumentParser()
parser.add_argument("--query", type=str, nargs='+')
args = parser.parse_args()

pagetitles = {}
for l in dfs.get_file_content('/wikipedia/__toc__'):
    splitted = l.split()
    doc_id = splitted[0]
    pagetitle = ' '.join(splitted[1:])
    pagetitles[doc_id] = pagetitle


doc_vector_len = {}
for l in dfs.get_file_content('/%s/doc_vector_len' % USERNAME):
    doc_id, doc_len = l.split()
    doc_vector_len[doc_id] = float(doc_len)


query_vector_length = 0
terms = Counter([term.lower() for term in args.query])
max_f = float(max(terms.values()))
term_dictionary = {}
for term in terms:
    tf = .5 * (1 + terms[term] / max_f)
    query_vector_length += tf ** 2
    if term[0] in term_dictionary:  # group query words by first letter
        term_dictionary[term[0]].append((term, tf))
    else:
        term_dictionary[term[0]] = [(term, tf)]
query_vector_length = sqrt(query_vector_length)


def tf_idf(tf, d):
    from math import log

    idf = log(corpus_size / d)
    return tf * idf


scores = {}
for first_letter in term_dictionary:
    term = term_dictionary[first_letter][0][0]
    shard = "".join(
        [l for l in
         metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, util.encode_term(term)[0:1]))])
    plists = json.JSONDecoder().decode(shard)
    for term, tf in term_dictionary[first_letter]:
        term = util.encode_term(term)
        if term in plists:
            for doc, term_tf in plists[term]:
                tfidf = tf_idf(term_tf, len(plists[term]))
                scores[doc] = tfidf * tf if doc not in scores else scores[doc] + tfidf * tf


# normalize
for doc_id in scores:
    scores[doc_id] /= query_vector_length * doc_vector_len[doc_id]

for doc_id in sorted(scores, key=scores.get, reverse=True)[:10]:
    print pagetitles[doc_id]

