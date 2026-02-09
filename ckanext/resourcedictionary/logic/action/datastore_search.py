# encoding: utf-8
from __future__ import annotations

from typing import Any, Dict, List

import ckan.plugins.toolkit as toolkit


@toolkit.chained_action
def datastore_search(original_action, context: dict[str, Any], data_dict: dict[str, Any]) -> dict[str, Any]:
    """Chained action that makes datastore_search return enriched field metadata.

    In CKAN >= 2.11, extensions can add per-field metadata to the Data Dictionary
    via IDataDictionaryForm. This metadata is reliably returned by datastore_info,
    but datastore_search may return only minimal field dicts (id/type) on some
    code paths. We merge the authoritative field dicts from datastore_info into
    the datastore_search response by matching on field id.

    Because this is a *chained_action*, it does not trigger NameConflict and it
    composes cleanly with other plugins chaining the same action.
    """
    result = original_action(context, data_dict)

    fields: List[Dict[str, Any]] = result.get('fields') or []
    if not fields:
        return result

    # If already enriched, do nothing
    if any(isinstance(f, dict) and f.get('info') for f in fields):
        return result

    res_id = data_dict.get('resource_id') or data_dict.get('id')
    if not res_id:
        return result

    try:
        datastore_info = toolkit.get_action('datastore_info')
        info_result = datastore_info(context, {'id': res_id})
    except Exception:
        return result

    enriched_fields = info_result.get('fields') or []
    if not enriched_fields:
        return result

    enriched_by_id = {
        ef.get('id'): ef for ef in enriched_fields
        if isinstance(ef, dict) and ef.get('id')
    }

    for f in fields:
        if not isinstance(f, dict):
            continue
        fid = f.get('id')
        if not fid:
            continue
        ef = enriched_by_id.get(fid)
        if not ef:
            continue
        # Merge everything except id/type (keep datastore_search's values)
        for k, v in ef.items():
            if k in ('id', 'type'):
                continue
            f[k] = v

    result['fields'] = fields
    return result
