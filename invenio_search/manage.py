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

"""
Commands configuring invenio search and the dependent services like
elasticsearch
"""

from elasticsearch.client import IndicesClient

import os

from invenio.ext.script import Manager

from invenio.base.globals import cfg

from flask import current_app

from flask_registry import PkgResourcesDirDiscoveryRegistry, RegistryProxy

from invenio_jsonschemas import api as schemas_api

from jsonschema_to_elasticmapping import mapping, templating

from werkzeug.local import LocalProxy

from .es import get_es_client
import mapping as es_mapping

manager = Manager(usage=__doc__)

elastic = Manager(usage='Commands creating and manipulating elasticsearch ' +
                  'index mappings and settings.')
manager.add_command('elasticsearch', elastic)

def get_es_mapping_templates_dir():
    """Retrieve the directory where the mapping templates are generated."""
    return current_app.config \
        .get('SEARCH_ES_MAPPING_TEMPLATES_DIR', os.path \
             .join(os.environ['VIRTUAL_ENV'],
                   'var/invenio-search/elasticsearch_index_config_templates'))

def get_es_mapping_dir():
    """Retrieve the directory where the elasticsearch mapping are generated."""
    return current_app.config \
        .get('SEARCH_ES_MAPPING_DIR', os.path \
             .join(os.environ['VIRTUAL_ENV'],
                   'var/invenio-search/elasticsearch_index_config'))

@elastic.command
def create_mappings():
    """Generate elasticsearch mappings and mapping templates"""
    # retrieve all existing jsonschemas
    schemas_urls = {}
    schemas = {}
    for schema_name in schemas_api.get_schemas():
        schemas[schema_name] = schemas_api.get_schema_data(schema_name)
        schemas_urls[schema_name] = schemas_api.internal_schema_url(schema_name)

    # discover all moldules' templates
    modules_templates = dict(RegistryProxy(
        'elasticsearch_index_config_templates',
        RecursiveDirDiscoveryRegistry,
        'elasticsearch_index_config_templates'
    ))

    # configuration used to generate the templates from jsonschemas
    config = mapping.ElasticMappingGeneratorConfig()

    # formats marking a jsonschema string field as an elasticsearch date
    sep = current_app.config.get('SEARCH_JSONSCHEMA_DATE_FORMATS_SEP', '||')
    jsonschema_date_formats = current_app.config \
        .get('SEARCH_JSONSCHEMA_DATE_FORMATS', 'date-time').split(sep)
    # elasticsearch accepted date formats
    es_mapping_date_formats = current_app.config \
        .get('SEARCH_ES_MAPPING_DATE_FORMATS', 'date_optional_time')
    # update the template generation config
    for js_format in jsonschema_date_formats:
        config.map_type('date', 'string', js_format,
                        { 'format': es_mapping_date_formats } if
                        es_mapping_date_formats else None )
    # default elasticsearch type corresponding to jsonschemas number
    es_mapping_number_type = current_app.config \
        .get('SEARCH_ES_MAPPING_NUMBER_TYPE', 'double')
    config.map_type(es_mapping_number_type, 'number')

    # default elasticsearch type corresponding to jsonschemas integer
    es_mapping_integer_type = current_app.config \
        .get('SEARCH_ES_MAPPING_INTEGER_TYPE', 'integer')
    config.map_type(es_mapping_integer_type, 'integer')

    # default elasticsearch type corresponding to jsonschemas boolean
    es_mapping_boolean_type = current_app.config \
        .get('SEARCH_ES_MAPPING_BOOLEAN_TYPE', 'boolean')
    config.map_type(es_mapping_boolean_type, 'boolean')

    es_mapping.create_mapping(get_es_mapping_templates_dir(),
                              get_es_mapping_dir(),
                              schemas, schemas_urls,
                              cfg['JSONSCHEMAS_BASE_SCHEMA'],
                              modules_templates, config)


@elastic.command
def send_configs():
    """Send modules' and generated mappings to elasticsearch."""
    # elasticsearch client to the index API
    es_client = IndicesClient(get_es_client(current_app))
    # Discover all elasticsearch mappings in modules
    modules_configs = dict(RegistryProxy(
        'elasticsearch_index_config',
        RecursiveDirDiscoveryRegistry,
        'elasticsearch_index_config'
    ))
    # send the mappings
    es_mapping.send_mappings(get_es_mapping_dir(), modules_configs, es_client)


class RecursiveDirDiscoveryRegistry(PkgResourcesDirDiscoveryRegistry):
    "Discover files in paths."

    def register(self, path):
        "Register (relative path -> absolute path) for each file recursively."
        dir, filename = os.path.split(path)
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for f in files:
                    relative_path = os.path \
                        .join(root[len(dir) + len(os.path.sep):], f)
                    absolute_path = os.path.join(root, f)
                    super(RecursiveDirDiscoveryRegistry, self).register(
                        (relative_path, absolute_path)
                    )
        else:
            return super(RecursiveDirDiscoveryRegistry, self).register(
                (filename, path)
            )


def main():
    """Run manager."""
    from invenio.base.factory import create_app
    app = create_app()
    manager.app = app
    manager.run()
