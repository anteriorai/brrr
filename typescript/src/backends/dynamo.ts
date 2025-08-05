import {
  CreateTableCommand,
  DeleteTableCommand,
  type DynamoDBClient,
} from "@aws-sdk/client-dynamodb";
import {
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import type { MemKey, Store } from "../store.ts";
import { CompareMismatchError, NotFoundError } from "../errors.ts";
import type { NativeAttributeValue } from "@aws-sdk/util-dynamodb";

export class Dynamo implements Store {
  private client: DynamoDBDocumentClient;
  private readonly tableName: string;

  public constructor(dynamoDbClient: DynamoDBClient, tableName: string) {
    this.client = DynamoDBDocumentClient.from(dynamoDbClient);
    this.tableName = tableName;
  }

  private key(memKey: MemKey): Record<string, NativeAttributeValue> {
    return {
      pk: memKey.callHash,
      sk: memKey.type,
    };
  }

  async has(memKey: MemKey): Promise<boolean> {
    const response = await this.client.send(
      new GetCommand({
        TableName: this.tableName,
        Key: this.key(memKey),
        ProjectionExpression: "pk",
      }),
    );
    return Boolean(response.Item);
  }

  async get(memKey: MemKey): Promise<Uint8Array> {
    const response = await this.client.send(
      new GetCommand({
        TableName: this.tableName,
        Key: this.key(memKey),
      }),
    );

    if (!response.Item || !response.Item.value) {
      throw new NotFoundError(memKey);
    }

    const value = response.Item.value;
    if (!(value instanceof Uint8Array)) {
      throw new Error("Stored value is not a binary blob");
    }
    return value;
  }

  async set(memKey: MemKey, value: Uint8Array): Promise<void> {
    await this.client.send(
      new PutCommand({
        TableName: this.tableName,
        Item: {
          ...this.key(memKey),
          value,
        },
      }),
    );
  }

  async delete(memKey: MemKey): Promise<void> {
    await this.client.send(
      new DeleteCommand({
        TableName: this.tableName,
        Key: this.key(memKey),
      }),
    );
  }

  async setNewValue(memKey: MemKey, value: Uint8Array): Promise<void> {
    try {
      await this.client.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: this.key(memKey),
          UpdateExpression: "SET #value = :value",
          ConditionExpression: "attribute_not_exists(#value)",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: { ":value": value },
        }),
      );
    } catch (err: unknown) {
      if (
        err instanceof Error &&
        err.name === "ConditionalCheckFailedException"
      ) {
        throw new CompareMismatchError(memKey);
      }
      throw err;
    }
  }

  async compareAndSet(
    memKey: MemKey,
    value: Uint8Array,
    expected: Uint8Array | null,
  ): Promise<void> {
    if (expected === null) {
      throw new Error("dynamo cannot CAS a missing value");
    }

    try {
      await this.client.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: this.key(memKey),
          UpdateExpression: "SET #value = :value",
          ConditionExpression: "#value = :expected",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: {
            ":value": value,
            ":expected": expected,
          },
        }),
      );
    } catch (err: unknown) {
      if (
        err instanceof Error &&
        err.name === "ConditionalCheckFailedException"
      ) {
        throw new CompareMismatchError(memKey);
      }
      throw err;
    }
  }

  async compareAndDelete(
    memKey: MemKey,
    expected: Uint8Array | null,
  ): Promise<void> {
    if (expected === null) {
      throw new Error("dynamo cannot CAS delete a missing value");
    }
    try {
      await this.client.send(
        new DeleteCommand({
          TableName: this.tableName,
          Key: this.key(memKey),
          ConditionExpression:
            "attribute_exists(#value) AND #value = :expected",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: { ":expected": expected },
        }),
      );
    } catch (err: unknown) {
      if (
        err instanceof Error &&
        err.name === "ConditionalCheckFailedException"
      ) {
        throw new CompareMismatchError(memKey);
      }
      throw err;
    }
  }

  async createTable(): Promise<void> {
    await this.client.send(
      new CreateTableCommand({
        TableName: this.tableName,
        KeySchema: [
          { AttributeName: "pk", KeyType: "HASH" },
          { AttributeName: "sk", KeyType: "RANGE" },
        ],
        AttributeDefinitions: [
          { AttributeName: "pk", AttributeType: "S" },
          { AttributeName: "sk", AttributeType: "S" },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 5,
        },
      }),
    );
  }

  async deleteTable(): Promise<void> {
    await this.client.send(
      new DeleteTableCommand({
        TableName: this.tableName,
      }),
    );
  }
}
