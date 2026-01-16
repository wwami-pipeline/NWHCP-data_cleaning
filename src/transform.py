#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Transformer: reads raw records from src/output/pipelinesurveydata.json (OUT_PUT_JSON_PATH),
applies src/mappings/mapping.json rules to produce canonical documents, and upserts into MongoDB
collection 'organization' (db 'mongodb').
"""

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from pymongo import MongoClient, ReplaceOne

OUT_PUT_JSON_PATH = "src/output/pipelinesurveydata.json"
MAPPING_JSON_PATH = "src/mappings/mapping.json"
MONGO_ADDR = os.environ.get("MONGO_ADDR", "mongodb://localhost:27017/")
DB_NAME = "mongodb"
SRC_COLLECTION = "surveys"  # not used for file-mode, kept for reference
DST_COLLECTION = "organization"

def normalize_mapping(v2: dict) -> dict:
    """
    Convert our mapping.json schema:
      - { "type": "single", "priority": [...] }
      - { "type": "checkbox_group", "members": { label: { "priority": [...] } } }
    Into the schema transform_record expects:
      - {"sources": [...]}
      - {"type": "checkbox", "sources": [...], "options": {...}}
    """
    fields = v2.get("fields", v2)
    out = {"fields": {}}

    for target, rule in fields.items():
        if not isinstance(rule, dict):
            # simple "string" or ["a","b"]
            out["fields"][target] = {"sources": [rule]} if isinstance(rule, str) else {"sources": rule}
            continue

        rtype = (rule.get("type") or "").lower()

        if rtype == "single":
            out["fields"][target] = {"sources": rule.get("priority", [])}

        elif rtype == "checkbox_group":
            sources, options = [], {}
            members = rule.get("members", {})
            for label, cfg in members.items():
                for src in cfg.get("priority", []):
                    sources.append(src)
                    options[src] = label
            out["fields"][target] = {"type": "checkbox", "sources": sources, "options": options}

        else:
            # fallback
            if "priority" in rule:
                out["fields"][target] = {"sources": rule["priority"]}
            else:
                out["fields"][target] = rule

    return out

def load_mapping(path: str = MAPPING_JSON_PATH) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_input(path: str = OUT_PUT_JSON_PATH, sample: Optional[int] = None) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if sample:
        return data[:sample]
    return data


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, (list, dict)) and len(value) == 0:
        return True
    return False


def is_truthy(value: Any) -> bool:
    if value is True:
        return True
    if value is False or value is None:
        return False
    if isinstance(value, (int, float)):
        return int(value) != 0
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "t", "yes", "y", "on")
    return False


def resolve_scalar(record: Dict[str, Any], sources: List[str]) -> Tuple[Optional[Any], Optional[str]]:
    """
    Return (value, source_key) - picks first non-missing value from sources in order.
    """
    for key in sources:
        if key in record and not is_missing(record.get(key)):
            return record.get(key), key
    return None, None


def resolve_checkbox_group(
    record: Dict[str, Any],
    sources: List[str],
    option_labels: Optional[Dict[str, str]] = None,
) -> Tuple[List[Any], List[str]]:
    """
    Consolidate checkbox group into array of values.
    - sources: list of checkbox field keys (individual boolean fields)
    - option_labels: optional dict mapping source_key -> label to include in array
    Returns (values_array, list_of_source_keys_used)
    """
    values = []
    used_keys = []
    for key in sources:
        if key not in record:
            continue
        val = record.get(key)
        if is_truthy(val):
            used_keys.append(key)
            if option_labels and key in option_labels:
                values.append(option_labels[key])
            else:
                # fallback: if key contains '___' (redcap checkbox naming), use suffix as label
                if "___" in key:
                    values.append(key.split("___", 1)[1])
                else:
                    values.append(key)
    return values, used_keys


def transform_record(record: Dict[str, Any], mapping: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply mapping rules to a single record and produce canonical doc.
    mapping expected structure (flexible):
      - mapping["fields"] : { canonical_field: rule }
    where rule can be:
      - list of source keys (priority order) -> scalar resolution
      - dict with:
          - "sources": [ ... ] (required)
          - "type": "checkbox" (optional)
          - "options": { source_key: label, ... } (optional, for checkbox)
          - "array_from": true (treat as array: include non-empty values from all sources)
    """
    canonical: Dict[str, Any] = {}

    fields = mapping.get("fields") or mapping  # allow mapping.json to be raw dict of fields

    for target_field, rule in fields.items():
        # normalize rule
        if isinstance(rule, list):
            rule_obj = {"sources": rule}
        elif isinstance(rule, dict):
            rule_obj = dict(rule)  # copy
            if "sources" not in rule_obj:
                # support the case where rule directly maps to single source string in dict
                # e.g., { "target": { "source": "field" } } -- treat "source" as single-element list
                if "source" in rule_obj:
                    rule_obj["sources"] = [rule_obj.pop("source")]
        else:
            # rule is a single string
            rule_obj = {"sources": [rule]}

        sources = rule_obj.get("sources", [])
        if not sources:
            canonical[target_field] = None
            continue

        rtype = rule_obj.get("type", "").lower()
        if rtype == "checkbox" or rule_obj.get("checkbox", False):
            options = rule_obj.get("options")
            values, _ = resolve_checkbox_group(record, sources, options)
            canonical[target_field] = values
        elif rule_obj.get("array_from", False):
            # collect non-empty values from all sources in order
            arr = []
            for key in sources:
                if key in record and not is_missing(record.get(key)):
                    arr.append(record.get(key))
            canonical[target_field] = arr
        else:
            value, _ = resolve_scalar(record, sources)
            canonical[target_field] = value

    # preserve _id
    raw_id = record.get("_id") or record.get("participant_id")
    canonical["_id"] = raw_id

    return canonical


def transform_many(records: List[Dict[str, Any]], mapping: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for rec in records:
        out.append(transform_record(rec, mapping))
    return out


def write_to_mongo(docs: List[Dict[str, Any]], mongo_addr: str = MONGO_ADDR) -> None:
    client = MongoClient(mongo_addr)
    db = client[DB_NAME]
    coll = db[DST_COLLECTION]

    ops = []
    for doc in docs:
        if doc.get("_id") is None:
            # skip documents without an id (shouldn't happen if input is from import_from_redcap.get_json)
            continue
        ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))

    if not ops:
        return

    # execute in batches to avoid very large bulk ops
    BATCH = 1000
    for i in range(0, len(ops), BATCH):
        batch_ops = ops[i : i + BATCH]
        coll.bulk_write(batch_ops)
