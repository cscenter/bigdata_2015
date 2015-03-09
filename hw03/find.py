# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import itertools
import json
import util
import sys
sys.path.append('../dfs/')
import client as dfs

metadata = dfs.CachedMetadata()


class TopK:

    def __init__(self, k):
        self.k = k
        self.docs = {}

    def add(self, doc_id, score):
        current_score = 0
        if doc_id in self.docs:
            current_score = self.docs[doc_id]
        self.docs[doc_id] = current_score + score

    def get(self):
        return list(itertools.islice(sorted(self.docs.items(),
                                            key=lambda x: x[1],
                                            reverse=True), self.k))


def docs_for_terms(group, terms):
    # Ваш псевдоним в виде строковой константы
    USERNAME = 'nightuser'
    shard = ''.join(list(metadata.get_file_content('/%s/posting_list/%s' %
                         (USERNAME, group))))
    plists = json.loads(shard)
    return [(term, plists[term]) for term in terms if term in plists]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', required=True)
    args = parser.parse_args()

    top_k = TopK(10)

    # plists = {}

    terms = [util.encode_term(x) for x in args.query.split()]
    terms.sort()
    for group, grouped_terms in itertools.groupby(terms, lambda x: x[0]):
        for term, docs in docs_for_terms(group, list(grouped_terms)):
            # plists[term] = docs
            for doc, score in docs:
                top_k.add(doc, score)

    for doc, _ in top_k.get():
        print(doc)


if __name__ == '__main__':
    main()
