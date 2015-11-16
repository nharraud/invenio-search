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

"""Test Invenio-search manage commands."""

import codecs

from invenio.base.utils import run_py_func

from invenio.base.globals import cfg
from invenio_search.manage import create_mappings, send_configs
from invenio_search.helpers import mkdir_p
from invenio.testsuite import InvenioTestCase, make_test_suite, run_test_suite

from flask import current_app
from flask import has_app_context, current_app

import json

from jsonschema_to_elasticmapping.mapping import (
    ElasticMappingGeneratorConfig
    , generate_type_mapping
)

from mock import patch

import os

import pytest

from .helpers import TemporaryDirectory, create_file


base_schema = 'records/gentest_base.json'
main_schema = 'records/gentest_main.json'
expected_type = 'records_gentest_main'

def get_schemas():
    return schemas.keys()

def get_schema_data(schema_name):
    return schemas[schema_name]

def internal_schema_url(schema_name):
    return schemas[schema_name]['id']

class MyClass(object):
    pass

from flask_registry import PackageRegistry

test_module_path = os.path.join(os.path.dirname(__file__),
                                'test_modules',
                                'test_elasticsearch_config')

class TestSearchManage(InvenioTestCase):

    @property
    def config(self):
        # add the test flask application to loaded packages
        cfg = super(TestSearchManage, self).config
        cfg['PACKAGES'] = [
            'tests.test_modules.test_elasticsearch_config',
            'invenio_jsonschemas',
            'invenio.base',
        ]
        return cfg

    def test_create_mappings(self):
        "Test inveniomanage search create_mappings"

        with TemporaryDirectory(2) as (template_dir, mapping_dir):
            # change the directory where the jinja templates are written
            cfg['SEARCH_ES_MAPPING_TEMPLATES_DIR'] = template_dir
            # change the directory where the final mapping are generated
            cfg['SEARCH_ES_MAPPING_DIR'] = mapping_dir
            # set the base mapping
            cfg['JSONSCHEMAS_BASE_SCHEMA'] = base_schema
            # set generation config
            cfg['SEARCH_JSONSCHEMA_DATE_FORMATS'] = \
                'my-test-date-format1||my-test-date-format2'
            cfg['SEARCH_ES_MAPPING_DATE_FORMATS']  = 'yyyy:MM||hh.mm.ss'
            cfg['SEARCH_ES_MAPPING_NUMBER_TYPE'] = 'float'
            cfg['SEARCH_ES_MAPPING_INTEGER_TYPE'] = 'long'
            cfg['SEARCH_ES_MAPPING_BOOLEAN_TYPE'] = 'boolean'

            # call the tested function
            run_py_func(create_mappings,
                        'search elasticsearch create_mappings')

            # build the expected mapping
            expected_mapping_path = os.path.join(mapping_dir, 'records',
                                                 'gentest_main', 'mapping.json')
            expected_mapping = self.build_expected_mapping(main_schema,
                                                           expected_type)
            # check that the generated mapping exists
            assert os.path.exists(expected_mapping_path)
            # open the generated mapping
            with codecs.open(expected_mapping_path, 'r', 'utf-8') \
                as mapping_file:
                gen_mapping = json.loads(mapping_file.read())
                # check that the mapping is as expected
                assert gen_mapping == expected_mapping


    def build_expected_mapping(self, schema_name, expected_type_name):
        """Build the expected mapping based on invenio config and the given
        jsonschema."""
        main_schema_path = os.path.join(test_module_path,
                                        'jsonschemas', 'records',
                                        'gentest_main.json')
        main_schema = None
        with codecs.open(main_schema_path, 'r', 'utf-8') as main_schema_file:
            main_schema = json.loads(main_schema_file.read())

        config = ElasticMappingGeneratorConfig()
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

        # generate the corresponding elasticsearch mapping
        mapping = generate_type_mapping(main_schema,
                                        'records_gettest_main',
                                        {},
                                        config)
        # modify the mapping according to the jinja template overriding the
        # generated one
        del mapping['properties']['removed_attr']
        mapping['properties']['additional_attr'] = { "type": "string" }
        return mapping


    # Patch elasticsearch.client.IndicesClient directly in invenio_search
    # manage as it has already been imported.
    @patch('invenio_search.manage.IndicesClient', autospec=True)
    def test_send_configs(self, MockIndicesClient):
        # mocked instance
        mock_indices_client = MockIndicesClient.return_value
        # path to the directory containing the non generated mapping and
        # settings
        module_config_dir_nongen = os.path.join(test_module_path,
                                                'elasticsearch_index_config',
                                                'records',
                                                'test_send_configs_nongen')
        # path to the directory containing the non generated settings for the
        # generated mapping
        module_config_dir_gen = os.path.join(test_module_path,
                                             'elasticsearch_index_config',
                                             'records',
                                             'test_send_configs_gen')
        # index names
        index_name_nongen = 'records_test_send_configs_nongen'
        index_name_gen = 'records_test_send_configs_gen'
        # mapping which will be written in the "generated" mappings directory
        mapping_gen = {
            '_all': True,
            'numeric_detection': False,
            'date_detection': False,
            'properties': {
                'generated_attr': {'type': 'string'},
            },
        }
        # fake index (non)existance method
        mock_indices_client.exists.side_effect = def_index_exists({
            index_name_nongen: True,
            index_name_gen: False
        })

        # load existing test mappings and settings
        mapping_nongen = None
        with codecs.open(os.path.join(module_config_dir_nongen, 'mapping.json'),
                         'r', 'utf-8') as mapping_file:
            mapping_nongen = json.loads(mapping_file.read())
        settings_nongen = None
        with codecs.open(os.path.join(module_config_dir_nongen, 'settings.json'),
                         'r', 'utf-8') as settings_file:
            settings_nongen = json.loads(settings_file.read())
        settings_gen = None
        with codecs.open(os.path.join(module_config_dir_gen, 'settings.json'),
                         'r', 'utf-8') as settings_file:
            settings_ngen = json.loads(settings_file.read())

        # create the mapping directory for the test
        with TemporaryDirectory() as (gen_mapping_dir,):
            mapping_gen_path = os.path.join(gen_mapping_dir, 'records',
                                            'test_send_configs_gen')
            mkdir_p(mapping_gen_path)
            # change the directory where the final mapping are generated
            cfg['SEARCH_ES_MAPPING_DIR'] = gen_mapping_dir
            # create the "generated" mapping
            create_file(mapping_gen_path, 'mapping.json',
                        json.dumps(mapping_gen))

            # call the tested function. We don't use run_py_func as the mocking
            # does not work with it
            send_configs()
            # check that the index existence was verified
            mock_indices_client.exists \
                .assert_any_call(index=index_name_nongen)
            mock_indices_client.exists \
                .assert_any_call(index=index_name_gen)

            # existing indexes should be closed->modified->opened
            mock_indices_client.open \
                .assert_any_call(index=index_name_nongen)
            mock_indices_client.put_mapping \
                .assert_any_call(index=index_name_nongen, doc_type='records',
                                 body=mapping_nongen)
            mock_indices_client.put_settings \
                .assert_any_call(index=index_name_nongen, body=settings_nongen)
            mock_indices_client.close \
                .assert_any_call(index=index_name_nongen)

            # non existing indexes should be created
            mock_indices_client.create \
                .assert_any_call(index=index_name_gen, body={
                    'settings': settings_gen,
                    'mappings': {
                        'records': mapping_gen
                    }
                })


def def_index_exists(config):
    def index_exists(index):
        return config[index]
    return index_exists


TEST_SUITE = make_test_suite(TestSearchManage)

if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
