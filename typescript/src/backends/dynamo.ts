import { CreateTableCommand, DeleteTableCommand, type DynamoDBClient, } from "@aws-sdk/client-dynamodb";
import { DeleteCommand, DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, } from "@aws-sdk/lib-dynamodb";
import type { MemKey, Store } from "../store.ts";
import type { NativeAttributeValue } from "@aws-sdk/util-dynamodb";

export class Dynamo implements Store {
  private readonly client: DynamoDBDocumentClient;
  private readonly tableName: string;

  public constructor(dynamoDbClient: DynamoDBClient, tableName: string) {
    this.client = DynamoDBDocumentClient.from(dynamoDbClient);
    this.tableName = tableName;
  }

  public async has(key: MemKey): Promise<boolean> {
    const response = await this.client.send(
      new GetCommand({
        TableName: this.tableName,
        Key: this.key(key),
        ProjectionExpression: "pk",
      }),
    );
    return !!response.Item;
  }

  public async get(key: MemKey): Promise<Uint8Array | undefined> {
    const response = await this.client.send(
      new GetCommand({
        TableName: this.tableName,
        Key: this.key(key),
      }),
    );
    return response.Item?.value;
  }

  public async set(key: MemKey, value: Uint8Array): Promise<void> {
    await this.client.send(
      new PutCommand({
        TableName: this.tableName,
        Item: {
          ...this.key(key),
          value,
        },
      }),
    );
  }

  public async delete(key: MemKey): Promise<boolean> {
    const response = await this.client.send(
      new DeleteCommand({
        TableName: this.tableName,
        Key: this.key(key),
        ReturnValues: "ALL_OLD",
      }),
    );
    return !!response.Attributes;
  }

  public async setNewValue(key: MemKey, value: Uint8Array): Promise<boolean> {
    try {
      await this.client.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: this.key(key),
          UpdateExpression: "SET #value = :value",
          ConditionExpression: "attribute_not_exists(#value)",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: { ":value": value },
        }),
      );
      return true
    } catch (err) {
      if (
        err instanceof Error &&
        err?.name === "ConditionalCheckFailedException"
      ) {
        return false
      }
      throw err;
    }
  }

  public async compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<boolean> {
    try {
      await this.client.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: this.key(key),
          UpdateExpression: "SET #value = :value",
          ConditionExpression: "#value = :expected",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: {
            ":value": value,
            ":expected": expected,
          },
        }),
      );
      return true
    } catch (err: unknown) {
      if (
        err instanceof Error &&
        err.name === "ConditionalCheckFailedException"
      ) {
        return false
      }
      throw err;
    }
  }

  public async compareAndDelete(
    key: MemKey,
    expected: Uint8Array,
  ): Promise<boolean> {
    try {
      await this.client.send(
        new DeleteCommand({
          TableName: this.tableName,
          Key: this.key(key),
          ConditionExpression:
            "attribute_exists(#value) AND #value = :expected",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: { ":expected": expected },
        }),
      );
      return true
    } catch (err: unknown) {
      if (
        err instanceof Error &&
        err.name === "ConditionalCheckFailedException"
      ) {
        return false
      }
      throw err;
    }
  }

  public async createTable(): Promise<void> {
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

  public async deleteTable(): Promise<void> {
    await this.client.send(
      new DeleteTableCommand({
        TableName: this.tableName,
      }),
    );
  }

  private key(key: MemKey): Record<string, NativeAttributeValue> {
    return {
      pk: key.callHash,
      sk: key.type,
    };
  }
}
