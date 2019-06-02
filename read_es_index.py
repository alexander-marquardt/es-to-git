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



def read_all_docs_from_es_index_and_persist_to_disk(index_name):
    scroll_over_all_docs(index_name)
