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

"""Elastic Search mapping generation and sending."""

import codecs

from flask import current_app

from flask_registry import PackageRegistry

import jinja2

import json

from jsonschema_to_elasticmapping import mapping, templating

import os

import shutil

import re

from .helpers import mkdir_p, split_all


def create_mapping(gen_templates_directory, gen_mappings_directory,
                   schemas, schemas_urls, json_base_schema,
                   modules_templates, template_gen_config):
    """Generate elasticsearch mappings and mapping templates.
    Used also for testing."""

    # remove all previously generated files
    if os.path.exists(gen_templates_directory):
        shutil.rmtree(gen_templates_directory)
    if os.path.exists(gen_mappings_directory):
        shutil.rmtree(gen_mappings_directory)
    # recreate the directories where the generated files will be written
    mkdir_p(os.path.join(gen_templates_directory))
    mkdir_p(os.path.join(gen_mappings_directory))

    # generate the templates
    templates = generate_mappings_templates(schemas, schemas_urls,
                                            json_base_schema,
                                            template_gen_config)

    # write the templates
    for rel_path, template in templates.iteritems():
        abs_path = os.path.join(gen_templates_directory, rel_path)
        mkdir_p(os.path.split(abs_path)[0])
        with codecs.open(abs_path, 'w', 'utf-8') \
            as es_template_file:
            es_template_file.write(template)

    # merge the modules_templates dict and the generated templates dict
    templates.update(modules_templates)

    # Create jinja environment. It will search for all templates in modules and
    # in the directory where generated templates have been put.
    jinja_loaders = [jinja2.PackageLoader(p, 'elasticsearch_index_config_templates')
                     for p in PackageRegistry(app=current_app)]
    jinja_loaders.append(jinja2.FileSystemLoader(gen_templates_directory))
    jinja_env = jinja2.Environment(loader = jinja2.ChoiceLoader(jinja_loaders))

    # Generate the final mapping from the import jinja templates
    for template in templates:
        root, file = os.path.split(template)
        if file != 'mapping.json':
            continue
        print('GENERATING elasticsearch mapping {}'.format(template))
        # Generate the mapping from the import jinja template
        es_mapping_str = jinja_env.get_template(template).render()
        # Remove all "null" values and format the mapping.
        # This enables extending a block and removing some of its fields by
        # just setting them as null.
        es_mapping = json \
            .dumps(mapping.clean_mapping(json.loads(es_mapping_str)), indent=4)
        # create the directory where the mapping will be written
        es_mapping_dir = os.path.join(gen_mappings_directory, root)
        mkdir_p(es_mapping_dir)
        es_mapping_path = os.path.join(es_mapping_dir, 'mapping.json')
        with codecs.open(es_mapping_path, 'w', 'utf-8') as es_mapping_file:
            es_mapping_file.write(es_mapping)


def generate_mappings_templates(schemas, schemas_urls, json_base_schema,
                                config):
    """Generate elasticsearch mapping templates from jsonschemas."""
    context_schemas = {}
    for schema_name, url in schemas_urls.iteritems():
        context_schemas[url] = schemas[schema_name]

    templates = {}
    # generate the jinja templates from jsonschemas
    for schema_path, url in schemas_urls.iteritems():
        # url = schemas_api.internal_schema_url(schema_path)
        #TODO: handle 'non record' schemas
        if (json_base_schema != schema_path and
                schema_path.startswith('records' + os.path.sep)):
            schema_dir, schema_file = os.path.split(schema_path)

            es_template_mapping = mapping \
                .generate_type_mapping(schemas[schema_path], url,
                                       context_schemas, config)
            schema_name = schema_file[:-len('.json')]

            # name of the type used in the jinja template. Remove characters
            type_name = re.sub('[^a-zA-Z0-9_]', '_', schema_name)
            es_template = templating.es_type_to_jinja(es_template_mapping,
                                                      type_name)

            # paths used by the jinja generator
            es_abs_template_relative_path = os.path.join(schema_dir, schema_name,
                                                         '_mapping.json')
            es_template_relative_path = os.path.join(schema_dir, schema_name,
                                                     'mapping.json')

            print('GENERATING elasticsearch mapping TEMPLATE {}' \
                  .format(es_template_relative_path))

            templates[es_abs_template_relative_path] = es_template
            templates[es_template_relative_path] = u'{{% extends "{}" %}}' \
                .format(es_abs_template_relative_path)

    return templates


def send_mappings(gen_mappings_directory, modules_configs, es_client):
    """Send modules' and generated mappings to elasticsearch.
    Used also for testing."""
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
        doc_type = split_all(rel_path)[0]
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
        if es_client.exists(index=index_name):
            print('UPDATING index {index} with mapping {mapping} and settings' \
                  ' {settings}' \
                  .format(index=index_name, mapping=abs_dir,
                          settings=(settings_path if index_settings else None)))

            es_client.close(index=index_name)
            if index_settings:
                es_client.put_settings(index=index_name, body=index_settings)
            es_client.put_mapping(index=index_name, doc_type=doc_type, body=index_mapping)
            es_client.open(index=index_name)
        else:
            print('CREATING index {index} with mapping {mapping} and settings' \
                  ' {settings}' \
                  .format(index=index_name, mapping=abs_dir,
                          settings=(settings_path if index_settings else None)))
            es_client.create(index=index_name, body={
                'settings': index_settings,
                'mappings': {
                    doc_type: index_mapping,
                }
            })
