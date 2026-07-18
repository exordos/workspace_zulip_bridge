import hashlib
import json
import unicodedata


class InvalidCanonicalValue(ValueError):
    pass


def _normalize(value: object) -> object:
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, bool) or value is None or isinstance(value, int):
        return value
    if isinstance(value, float):
        raise InvalidCanonicalValue("Floating-point JSON values are forbidden")
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise InvalidCanonicalValue("JSON object keys must be strings")
        return {_normalize(key): _normalize(item) for key, item in value.items()}
    raise InvalidCanonicalValue("Unsupported JSON value")


def canonical_json(value: dict[str, object]) -> bytes:
    return json.dumps(
        _normalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def operation_digest(record: dict[str, object]) -> str:
    value = {
        "account_uuid": record["account_uuid"],
        "causal_lane": record["causal_lane"],
        "operation": record["operation"],
        "operation_uuid": record["operation_uuid"],
        "origin": record["origin"],
        "predecessor_operation_uuid": record["predecessor_operation_uuid"],
        "project_uuid": record["project_uuid"],
        "sequence": record["sequence"],
    }
    return hashlib.sha256(canonical_json(value)).hexdigest()
