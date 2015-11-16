# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

""" Elastic Search integration."""

from __future__ import absolute_import

from elasticsearch import Elasticsearch

from elasticsearch.connection import RequestsHttpConnection

from invenio.celery import celery

es = None


def create_index(sender, **kwargs):
    """Create or recreate the elasticsearch index for records."""
    es.indices.delete(index='records', ignore=404)

    with open('result/elasticmapping.json') as mapping_file:
        mapping = json.load(mapping_file)
        es.indices.create(index='records', body=mapping)


def delete_index(sender, **kwargs):
    """Create the elasticsearch index for records."""
    es.indices.delete(index='records', ignore=404)

def get_es_client(app):
    """Initialize the Elasticsearch client if it is not already created
    and return it."""
    global es

    if not es:
        es = Elasticsearch(
            app.config.get('ES_HOSTS', None),
            connection_class=RequestsHttpConnection
        )
    return es

def setup_app(app):
    """Set up the extension for the given app."""
    from invenio.base import signals
    from invenio.base.scripts.database import recreate, drop, create

    from invenio_records.models import RecordMetadata

    from sqlalchemy.event import listens_for

    es = get_es_client(app)

    signals.pre_command.connect(delete_index, sender=drop)
    signals.pre_command.connect(create_index, sender=create)
    signals.pre_command.connect(delete_index, sender=recreate)
    signals.pre_command.connect(create_index, sender=recreate)

    @listens_for(RecordMetadata, 'after_insert')
    @listens_for(RecordMetadata, 'after_update')
    def new_record(mapper, connection, target):
        index_record.delay(target.id)

    # FIXME add after_delete

    from invenio_collections.models import Collection

    @listens_for(Collection, 'after_insert')
    @listens_for(Collection, 'after_update')
    def new_collection(mapper, connection, target):
        if target.dbquery is not None:
            index_collection_percolator.delay(target.name, target.dbquery)

    # FIXME add after_delete


@celery.task
def index_record(recid):
    """Index a record in elasticsearch."""
    from invenio_records.models import RecordMetadata
    record = RecordMetadata.query.get(recid)
    es.index(
        index='records',
        doc_type='record',
        body=record.json,
        id=record.id
    )


@celery.task
def index_collection_percolator(name, dbquery):
    """Create an elasticsearch percolator for a given query."""
    from invenio_search.api import Query
    from invenio_search.walkers.elasticsearch import ElasticSearchDSL
    es.index(
        index='records',
        doc_type='.percolator',
        body={'query': Query(dbquery).query.accept(ElasticSearchDSL())},
        id=name
    )
