import { describe, test } from "node:test";
import { storeContractTest } from "../store.test.ts";
import { Dynamo } from "./dynamo.ts";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { randomUUID } from "node:crypto";

await describe(import.meta.filename, async () => {
  await test(Dynamo.name, async () => {
    const client = new DynamoDBClient();

    await storeContractTest(async () => {
      const tableName = `brrr_test_${randomUUID()}`;
      const dynamoStore = new Dynamo(client, tableName);
      await dynamoStore.createTable();
      return dynamoStore;
    });
  });
});
