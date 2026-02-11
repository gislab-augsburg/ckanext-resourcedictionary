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
# NOTE: CKAN (>=2.11) does not allow action name conflicts.
# We therefore do NOT register an IActions override for core actions like
# 'datastore_search'. Instead, we monkeypatch the action registry after
# all plugins are loaded (in update_config).


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
        from ckanext.resourcedictionary.logic.action.datastore_search import datastore_search
        return {
            'resource_dictionary_create': resource_dictionary_create,
            'datastore_search': datastore_search,
        }

    # IDataDictionaryForm
    def update_datastore_create_schema(self, schema: Schema) -> Schema:
        """Extend datastore_create schema so extra per-field metadata is
        validated and stored in plugin_data (CKAN >= 2.11).
        """
        ignore_missing = toolkit.get_validator('ignore_missing')
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
            fields_schema[key] = [ignore_missing, unicode_only, to_rd]

        schema['fields'] = fields_schema
        return schema

    def update_datastore_info_field(
        self, field: dict[str, Any], plugin_data: dict[str, Any]
    ):
        """Expose stored plugin_data under field['info'].

        Compatibility behavior:
        - Always provide the known keys in field['info'] so downstream scripts that
          expect them (even as empty strings) don't break.
        - Preserve empty-string values instead of dropping them.
        """
        data = plugin_data.get(PLUGIN_KEY, {}) or {}
        info = field.get('info') or {}
        if not isinstance(info, dict):
            info = {}

        # Merge plugin_data from this plugin (may include empty strings)
        if isinstance(data, dict):
            info.update(data)

        # Ensure stable presence of expected keys
        for k in ('condition', 'db_type', 'label', 'notes', 'opendata'):
            if k not in info or info[k] is None:
                info[k] = ''

        field['info'] = info
        return field
