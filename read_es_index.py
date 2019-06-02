import logging
import json
import os

from elasticsearch import Elasticsearch

es = Elasticsearch(["localhost:9200"])
output_dir = "/Users/arm/PycharmProjects/es-indices-backup"


def scroll_over_all_docs(index_name):

    try:
        data = es.search(index=index_name, scroll='1m',  body={"query": {"match_all": {}}})
        sid = data['_scroll_id']

        while True:
            scroll_size = len(data['hits']['hits'])
            if scroll_size <= 0:
                break

            hits = data['hits']['hits']
            for doc in hits:
                print("document found: %s" % doc)
                filename = os.path.join(output_dir, "idx-{}-_id-{}".format(doc['_index'], doc['_id']))
                with open(filename, 'w') as outfile:
                    json.dump(doc, outfile)

            data = es.scroll(scroll_id=sid, scroll='1m')

    except Exception as e:
        logging.exception(e)


def loop_over_hashes_and_remove_duplicates():
    # Search through the hash of doc values to see if any
    # duplicate hashes have been found
    for hashval, array_of_ids in dict_of_duplicate_docs.items():
        if len(array_of_ids) > 1:
            print("********** Duplicate docs hash=%s **********" % hashval)
            # Get the documents that have mapped to the current hasval
            matching_docs = es.mget(index="stocks", doc_type="doc", body={"ids": array_of_ids})
            for doc in matching_docs['docs']:
                # In order to remove the possibility of hash collisions,
                # write code here to check all fields in the docs to
                # see if they are truly identical - if so, then execute a
                # DELETE operation on all except one.
                # In this example, we just print the docs.
                print("doc=%s\n" % doc)


def read_all_docs_from_es_index_and_persist_to_disk(index_name):
    scroll_over_all_docs(index_name)
