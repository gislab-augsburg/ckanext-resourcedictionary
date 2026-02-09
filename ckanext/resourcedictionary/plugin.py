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

        # Make datastore_search field metadata deterministic.
        #
        # Some deployments (eg uWSGI with lazy-apps) can exhibit behaviour where
        # datastore_search sometimes returns only minimal field dicts
        # (id/type) even though datastore_info correctly includes enriched
        # info/plugin_data. CKAN does not allow action-name conflicts via
        # IActions, so we patch the action registry in-place.
        self._patch_datastore_search_action()

    # IBlueprint
    def get_blueprint(self):
        return [resource_dictionary]

    # IActions
    def get_actions(self):
        return {
            'resource_dictionary_create': resource_dictionary_create,
        }

    def _patch_datastore_search_action(self) -> None:
        """Monkeypatch ckan.logic.action._actions['datastore_search'].

        We merge enriched field metadata from datastore_info into the
        datastore_search response so clients always see field['info'].
        """
        try:
            import ckan.logic.action as logic_action  # type: ignore
        except Exception:
            return

        actions = getattr(logic_action, '_actions', None)
        if not isinstance(actions, dict):
            return

        # Idempotency guard
        sentinel = '__resourcedictionary_patched__'
        if actions.get(sentinel):
            return

        original = actions.get('datastore_search')
        datastore_info = actions.get('datastore_info')
        if not callable(original) or not callable(datastore_info):
            return

        def patched_datastore_search(context, data_dict):
            result = original(context, data_dict)

            try:
                # Defensive: only post-process successful responses
                fields = result.get('fields')
                if not isinstance(fields, list) or not fields:
                    return result

                # If already enriched, do nothing
                if any(isinstance(f, dict) and f.get('info') for f in fields):
                    return result

                # Pull the authoritative enriched schema from datastore_info
                info_res = datastore_info(context, {'id': data_dict.get('id')})
                info_fields = info_res.get('fields')
                if not isinstance(info_fields, list) or not info_fields:
                    return result

                by_id = {
                    f.get('id'): f
                    for f in info_fields
                    if isinstance(f, dict) and f.get('id')
                }

                merged = []
                for f in fields:
                    if not isinstance(f, dict):
                        merged.append(f)
                        continue
                    fid = f.get('id')
                    src = by_id.get(fid)
                    if isinstance(src, dict):
                        # Copy any extra keys from datastore_info (incl. info)
                        out = dict(f)
                        for k, v in src.items():
                            if k not in out:
                                out[k] = v
                        merged.append(out)
                    else:
                        merged.append(f)

                result['fields'] = merged
            except Exception:
                # Never break the API on enrichment failures
                return result

            return result

        actions['datastore_search'] = patched_datastore_search
        actions[sentinel] = True

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
