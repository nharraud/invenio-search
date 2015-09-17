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

""" Elastic Search integration. invenio manage file"""

import elasticsearch

import errno

import os

import codecs

from invenio.ext.script import Manager

from invenio.base.globals import cfg

import jinja2

import json

from flask import current_app

from flask_registry import PkgResourcesDirDiscoveryRegistry, \
    RegistryProxy, ImportPathRegistry, ListRegistry, DictRegistry, \
    EntryPointRegistry, PackageRegistry

from invenio_jsonschemas import api as schemas_api

from jsonschema_to_elasticmapping import mapping, templating

import re

import shutil

from werkzeug.local import LocalProxy

from .es import get_es_client

manager = Manager(usage=__doc__)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


# Directory where the mapping templates are generated
gen_templates_directory = os.path.join(os.environ['VIRTUAL_ENV'],
                                       'var/invenio-search/elasticsearch_index_config_templates')
# Directory where the elasticsearch mapping are generated
gen_mappings_directory = os.path.join(os.environ['VIRTUAL_ENV'],
                                      'var/invenio-search/elasticsearch_index_config')

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

@manager.command
def create_mappings():
    """Generate elasticsearch mappings and mapping templates"""
    # retrieve all existing jsonschemas
    context_schemas = {}
    schemas = schemas_api.get_schemas()
    for schema_name in schemas:
        url = schemas_api.internal_schema_url(schema_name)
        schema = schemas_api.get_schema_data(schema_name)
        context_schemas[url] = schema

    # remove all previously generated files
    shutil.rmtree(gen_templates_directory)
    shutil.rmtree(gen_mappings_directory)
    # recreate the directories where the generated files will be written
    mkdir_p(os.path.join(gen_templates_directory))
    mkdir_p(os.path.join(gen_mappings_directory))

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

    # generate the jinja templates from jsonschemas
    for schema_path in schemas:
        url = schemas_api.internal_schema_url(schema_path)
        #TODO: handle 'non record' schemas
        if (cfg['JSONSCHEMAS_BASE_SCHEMA'] != schema_path and
                schema_path.startswith('records/')):
            schema_dir, schema_file = os.path.split(schema_path)

            es_template_mapping = mapping \
                .generate_type_mapping(context_schemas[url], url,
                                       context_schemas, config)
            schema_name = schema_file[:-len('.json')]

            # name of the type used in the jinja template. Remove characters
            type_name = re.sub('[^a-zA-Z0-9_]', '_', schema_name)
            es_template = templating.es_type_to_jinja(es_template_mapping,
                                                      type_name)

            es_template_directory = os.path.join(gen_templates_directory,
                                                 schema_dir, schema_name)
            mkdir_p(es_template_directory)

            # paths where the templates are written.
            es_template_path = os.path.join(es_template_directory,
                                            'mapping.json')
            es_abstract_template_path = os.path.join(es_template_directory,
                                                     '_mapping.json')

            # paths used by the jinja generator
            es_abs_template_relative_path = os.path.join(schema_dir, schema_name,
                                                         '_mapping.json')
            es_template_relative_path = os.path.join(schema_dir, schema_name,
                                                     'mapping.json')

            print('GENERATING elasticsearch mapping TEMPLATE {}' \
                  .format(es_template_relative_path))

            with codecs.open(es_abstract_template_path, 'w', 'utf-8') \
              as es_template_file:
                es_template_file.write(es_template)

            with codecs.open(es_template_path, 'w', 'utf-8') \
              as es_template_file:
                es_template_file.write(u'{{% extends "{}" %}}'
                                       .format(es_abs_template_relative_path))
                if schema_path not in modules_templates:
                    modules_templates[es_template_relative_path] = \
                        es_template_path

    # Create jinja environment. It will search for all templates in modules and
    # in the directory where generated templates have been put.
    jinja_loaders = [jinja2.PackageLoader(p, 'elasticsearch_index_config_templates')
                     for p in PackageRegistry(app=current_app)]
    jinja_loaders.append(jinja2.FileSystemLoader(gen_templates_directory))
    jinja_env = jinja2.Environment(loader = jinja2.ChoiceLoader(jinja_loaders))

    # Generate the final mapping from the import jinja templates
    for template in modules_templates:
        root, file = os.path.split(template)
        if file != 'mapping.json':
            continue
        print('GENERATING elasticsearch mapping {}'.format(template))
        # Generate the mapping from the import jinja template
        es_mapping_str = jinja_env.get_template(template).render()
        # Remove all "null" values and format the mapping.
        # This enables extending a block and removing some of its fields by
        # just setting them as null.
        es_mapping = json.dumps(__clean_mapping(json.loads(es_mapping_str)),
                                indent=4)
        # create the directory where the mapping will be written
        es_mapping_dir = os.path.join(gen_mappings_directory, root)
        mkdir_p(es_mapping_dir)
        es_mapping_path = os.path.join(es_mapping_dir, 'mapping.json')
        with codecs.open(es_mapping_path, 'w', 'utf-8') as es_mapping_file:
            es_mapping_file.write(es_mapping)


@manager.command
def send_mappings():
    """Send modules' and generated mappings to elasticsearch"""
    es = elasticsearch.client.IndicesClient(get_es_client(current_app))
    modules_configs = dict(RegistryProxy(
        'elasticsearch_index_config',
        RecursiveDirDiscoveryRegistry,
        'elasticsearch_index_config'
    ))

    # add the generated mapping files
    for root, dirs, files in os.walk(gen_mappings_directory):
        for f in files:
            relative_path = os.path \
                .join(root[len(gen_mappings_directory) + len(os.path.sep):], f)
            absolute_path = os.path.join(root, f)
            if relative_path not in modules_configs:
                modules_configs[relative_path] = absolute_path

    # for each mapping, create or update the index. Send also the corresponding
    # settings
    for rel_path, abs_path in modules_configs.iteritems():
        rel_dir, file_name = os.path.split(rel_path)
        abs_dir = os.path.split(abs_path)[0]
        if file_name != 'mapping.json':
            continue
        doc_type = __split_all(rel_path)[0]
        settings_path = os.path.join(abs_dir, 'settings.json')

        # Transform the path so that it can be used as an index name
        # Index names cannot contain all characters.
        index_name = re.sub('[^a-zA-Z0-9_.-]', '_', rel_dir).lower()
        index_mapping = None
        index_settings = None
        with codecs.open(abs_path, 'r', 'utf-8') as index_mapping_file:
            index_mapping = json.loads(index_mapping_file.read())
        if os.path.isfile(settings_path):
            with codecs.open(settings_path, 'r', 'utf-8') as settings_file:
                index_settings = json.loads(settings_file.read())

        # if the index exists then update its configuration else create it
        if es.exists(index=index_name):
            es.close(index=index_name)
            if index_settings:
                es.put_settings(index=index_name, body=index_settings)
            es.put_mapping(index=index_name, doc_type=doc_type, body=index_mapping)
            es.open(index=index_name)
        else:
            es.create(index=index_name, body={
                'settings': index_settings,
                'mappings': {
                    doc_type: index_mapping,
                }
            })


def __split_all(path):
    """Split folders in a given path and return them as a list."""
    folders = []
    while len(path) > 0:
        path, folder = os.path.split(path)
        if folder != '':
            folders.append(folder)
    folders.reverse()
    return folders


def __clean_mapping(mapping):
    """Recursively remove all fields set to None in a dict and child dicts."""
    return {key: (value if not isinstance(value, dict)
                  else __clean_mapping(value))
            for (key, value) in mapping.iteritems() if value is not None}


def main():
    """Run manager."""
    from invenio.base.factory import create_app
    app = create_app()
    manager.app = app
    manager.run()
