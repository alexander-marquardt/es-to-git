"""
This program is designed to automate the backup process for certain Elasticsearch indices
such as .watches or .logstash-*.

This program can be executed via a cron job to periodically check for updates, and will then
write those updates to git. Make a separate call to this program for each index that is to
be backed up.


This could also in theory be used for backing up other indices
to github, but for other use cases, the overhead should be monitored carefully.

See:
https://www.elastic.co/guide/en/elasticsearch/reference/current/watcher-api-put-watch.html
 https://www.elastic.co/guide/en/logstash/master/configuring-centralized-pipelines.html
"""
import os
import argparse

import read_es_index

# Parse the command line options, to determine which section(s) of the code will be executed
# and connect to Elasticsearch
def initial_setup():
    """
    Extract the command line parameters
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--index_name', default=None, metavar='', dest='index',
                        help='-i <index to backup>')

    parsed_args = parser.parse_args()
    print('Executing %s on index %s' % (os.path.basename(__file__), parsed_args.index))

    return parsed_args

def main():

    parsed_args = initial_setup()

    if not parsed_args.index:
        print('Pass in an index name. eg. "%s -i .watches"' % os.path.basename(__file__))
        exit(1)

    read_es_index.read_all_docs_from_es_index_and_persist_to_disk(parsed_args.index)

main()