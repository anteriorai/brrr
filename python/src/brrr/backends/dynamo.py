from __future__ import annotations

import logging
import typing

from ..store import CompareMismatch, MemKey, NotFoundError, Store

if typing.TYPE_CHECKING:
    from types_aiobotocore_dynamodb import DynamoDBClient


logger = logging.getLogger(__name__)


# The frame table layout is:
#
#   pk: MEMO_KEY
#   sk: "pending_returns"
#   parents: list[str]
#
# OR
#
#   pk: MEMO_KEY
#   sk: "call"
#   task: The task name
#   argv: bytes (pickled)
#
# OR
#
#   pk: MEMO_KEY
#   sk: "value"
#   value: bytes (pickled)
#
# TODO It is possible we'll add versioning in there as pk or somethin
class DynamoDbMemStore(Store):
    client: DynamoDBClient
    table_name: str

    def key(self, mem_key: MemKey) -> dict[str, dict[str, str]]:
        return {"pk": {"S": mem_key.call_hash}, "sk": {"S": mem_key.type}}

    def __init__(self, client: DynamoDBClient, table_name: str):
        self.client = client
        self.table_name = table_name

    async def has(self, key: MemKey) -> bool:
        return "Item" in await self.client.get_item(
            TableName=self.table_name,
            Key=self.key(key),
        )

    async def get(self, key: MemKey) -> bytes:
        response = await self.client.get_item(
            TableName=self.table_name,
            Key=self.key(key),
        )
        if "Item" not in response:
            logger.debug(f"getting key: {key}: not found")
            raise NotFoundError(key)
        logger.debug(f"getting key: {key}: found")
        return response["Item"]["value"]["B"]

    async def set(self, key: MemKey, value: bytes) -> None:
        await self.client.put_item(
            TableName=self.table_name, Item={**self.key(key), "value": {"B": value}}
        )

    async def delete(self, key: MemKey) -> None:
        await self.client.delete_item(
            TableName=self.table_name,
            Key=self.key(key),
        )

    async def set_new_value(self, key: MemKey, value: bytes) -> None:
        """Set a value, ensuring none was previously set"""
        try:
            await self.client.update_item(
                TableName=self.table_name,
                Key=self.key(key),
                UpdateExpression="SET #value = :value",
                ExpressionAttributeNames={"#value": "value"},
                ExpressionAttributeValues={":value": {"B": value}},
                ConditionExpression="attribute_not_exists(#value)",
            )
        except self.client.exceptions.ConditionalCheckFailedException as e:
            raise CompareMismatch() from e

    async def compare_and_set(self, key: MemKey, value: bytes, expected: bytes) -> None:
        if expected is None:
            raise ValueError("dynamo cannot CAS a missing value")
        try:
            await self.client.update_item(
                TableName=self.table_name,
                Key=self.key(key),
                UpdateExpression="SET #value = :value",
                ExpressionAttributeNames={"#value": "value"},
                ExpressionAttributeValues={
                    ":value": {"B": value},
                    ":expected": {"B": expected},
                },
                ConditionExpression="#value = :expected",
            )
        except self.client.exceptions.ConditionalCheckFailedException as e:
            raise CompareMismatch() from e

    async def compare_and_delete(self, key: MemKey, expected: bytes) -> None:
        if expected is None:
            raise ValueError("dynamo cannot CAS delete a missing value")
        try:
            await self.client.delete_item(
                TableName=self.table_name,
                Key=self.key(key),
                ConditionExpression="attribute_exists(#value) AND #value = :expected",
                # value is a reserved word in DynamoDB
                ExpressionAttributeNames={"#value": "value"},
                ExpressionAttributeValues={":expected": {"B": expected}},
            )
        except self.client.exceptions.ConditionalCheckFailedException as e:
            raise CompareMismatch() from e

    async def create_table(self) -> None:
        try:
            await self.client.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                ],
                # TODO make this configurable? Should this method even exist?
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
        except self.client.exceptions.ResourceInUseException:
            pass
