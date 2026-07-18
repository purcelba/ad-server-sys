"""Materialization: writes a job's computed feature values into
DynamoDB-local — the durable online feature store.

Single-table design: PK `entity_key` (e.g. `user#u_0001`), SK
`feature_name`, attributes `value` + `computed_at`. Table is ephemeral
(dynamodb-local runs -inMemory), so `create_table_if_not_exists()` runs at
the start of every materializing run.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import boto3
import polars as pl

TABLE_NAME = "features"
DYNAMODB_ENDPOINT = "http://localhost:8000"

_ID_COL_BY_ENTITY = {"user": "user_id", "ad": "campaign_id"}
_KEY_PREFIX_BY_ENTITY = {"user": "user", "ad": "ad"}


def get_resource():
    return boto3.resource(
        "dynamodb",
        endpoint_url=DYNAMODB_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )


def create_table_if_not_exists(resource=None) -> None:
    resource = resource or get_resource()
    existing = {table.name for table in resource.tables.all()}
    if TABLE_NAME in existing:
        return
    table = resource.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "entity_key", "KeyType": "HASH"},
            {"AttributeName": "feature_name", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "entity_key", "AttributeType": "S"},
            {"AttributeName": "feature_name", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()


def _to_dynamo_value(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _to_dynamo_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_dynamo_value(v) for v in value]
    return value


def materialize(
    entity: str,
    df: pl.DataFrame,
    feature_names: list[str],
    computed_at: dt.datetime,
    resource=None,
) -> int:
    """Write every (entity, feature_name) pair in df to DynamoDB-local.

    Only writes values the job actually computed — no artificial nulls for
    entities the job didn't produce a row for (those stay 'missing',
    resolved to the registry default at read time, per CLAUDE.md's
    defaults policy).

    Returns the number of items written.
    """
    resource = resource or get_resource()
    create_table_if_not_exists(resource)
    table = resource.Table(TABLE_NAME)

    id_col = _ID_COL_BY_ENTITY[entity]
    prefix = _KEY_PREFIX_BY_ENTITY[entity]

    count = 0
    with table.batch_writer() as batch:
        for row in df.iter_rows(named=True):
            entity_key = f"{prefix}#{row[id_col]}"
            for feature_name in feature_names:
                value = row.get(feature_name)
                if value is None:
                    continue
                batch.put_item(
                    Item={
                        "entity_key": entity_key,
                        "feature_name": feature_name,
                        "value": _to_dynamo_value(value),
                        "computed_at": computed_at.isoformat(),
                    }
                )
                count += 1
    return count
