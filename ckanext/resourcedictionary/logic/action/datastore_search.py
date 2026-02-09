# encoding: utf-8
from __future__ import annotations

from typing import Any, Dict, List, Optional

import ckan.plugins.toolkit as toolkit


def _import_core_action(name: str):
    """Import a core datastore action function directly to avoid recursion
    through toolkit.get_action when we override datastore_search.
    """
    # CKAN 2.9+ uses ckanext.datastore.logic.action
    try:
        from ckanext.datastore.logic import action as datastore_action  # type: ignore
    except Exception as e:
        raise
    fn = getattr(datastore_action, name, None)
    if fn is None:
        # Some CKAN versions expose actions in ckanext.datastore.logic.action
        try:
            from ckanext.datastore.logic.action import __dict__ as action_dict  # type: ignore
            fn = action_dict.get(name)
        except Exception:
            fn = None
    if fn is None:
        raise AttributeError(f"Could not import core datastore action '{name}'")
    return fn


_core_datastore_search = _import_core_action('datastore_search')
_core_datastore_info = _import_core_action('datastore_info')


def datastore_search(context: dict[str, Any], data_dict: dict[str, Any]) -> dict[str, Any]:
    """Wrapper around core datastore_search that ensures data dictionary
    metadata (field['info']) is consistently returned.

    Why this exists (CKAN 2.11 in the wild):
    - datastore_info returns enriched fields (incl. plugin_data -> info)
    - datastore_search may return only id/type depending on backend path
      or caching, especially with multi-process uWSGI + lazy-apps.
    This wrapper merges the datastore_info field dicts into the
    datastore_search field dicts by matching on field id.
    """
    result = _core_datastore_search(context, data_dict)

    fields: List[Dict[str, Any]] = result.get('fields') or []
    if not fields:
        return result

    # If any field already has info, assume enrichment already happened.
    if any(isinstance(f, dict) and f.get('info') for f in fields):
        return result

    res_id = data_dict.get('resource_id') or data_dict.get('id')
    if not res_id:
        return result

    try:
        info_result = _core_datastore_info(context, {'id': res_id})
    except Exception:
        # Never fail the search because of info enrichment
        return result

    enriched_fields = (info_result.get('fields') or [])
    if not enriched_fields:
        return result

    enriched_by_id = {
        ef.get('id'): ef for ef in enriched_fields
        if isinstance(ef, dict) and ef.get('id')
    }

    for f in fields:
        fid = f.get('id') if isinstance(f, dict) else None
        if not fid:
            continue
        ef = enriched_by_id.get(fid)
        if not ef:
            continue
        # Merge all keys except id/type (keep datastore_search's values)
        for k, v in ef.items():
            if k in ('id', 'type'):
                continue
            f[k] = v

    result['fields'] = fields
    return result
