# encoding: utf-8
from __future__ import annotations

from typing import Any, cast

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckan.common import CKANConfig
from ckan.types import Schema, ValidatorFactory
from ckanext.datastore.interfaces import IDataDictionaryForm

from ckanext.resourcedictionary.views.resource_dictionary import resource_dictionary
from ckanext.resourcedictionary.logic.action.create import resource_dictionary_create


PLUGIN_KEY = 'resourcedictionary'


class ResourcedictionaryPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IActions)
    plugins.implements(IDataDictionaryForm)

    # IConfigurer
    def update_config(self, config: CKANConfig):
        toolkit.add_template_directory(config, 'templates')
        toolkit.add_public_directory(config, 'public')
        toolkit.add_resource('assets', 'resourcedictionary')

        # Index resource dictionary metadata in Solr.
        config['ckan.extra_resource_fields'] = (
            'dictionary_fields dictionary_labels dictionary_notes'
        )

    # IBlueprint
    def get_blueprint(self):
        return [resource_dictionary]

    # IActions
    def get_actions(self):
        return {
            'resource_dictionary_create': resource_dictionary_create
        }

    # IDataDictionaryForm
    def update_datastore_create_schema(self, schema: Schema) -> Schema:
        """Extend datastore_create schema so extra per-field metadata is
        validated and stored in plugin_data (CKAN >= 2.11).
        """
        ignore_empty = toolkit.get_validator('ignore_empty')
        unicode_only = toolkit.get_validator('unicode_only')
        to_datastore_plugin_data = cast(
            ValidatorFactory, toolkit.get_validator('to_datastore_plugin_data')
        )
        to_rd = to_datastore_plugin_data(PLUGIN_KEY)

        fields_schema = cast(Schema, schema.get('fields', {}))

        for key in (
            'label',
            'notes',
            'condition',
            'opendata',
            'db_type',
            'type_override',
        ):
            fields_schema[key] = [ignore_empty, unicode_only, to_rd]

        schema['fields'] = fields_schema
        return schema

    def update_datastore_info_field(
        self, field: dict[str, Any], plugin_data: dict[str, Any]
    ):
        """Expose stored plugin_data under field['info'] for backwards compatibility."""
        data = plugin_data.get(PLUGIN_KEY, {})
        if data:
            info = field.get('info') or {}
            if not isinstance(info, dict):
                info = {}
            info.update(data)
            field['info'] = info
        return field
