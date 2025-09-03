import { suite } from "node:test";
import { Dynamo } from "./dynamo.ts";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { storeContractTest } from "../store.test.ts";
import { randomUUID } from "node:crypto";

await suite(import.meta.filename, async () => {
  const client = new DynamoDBClient();

  let dynamo: Dynamo;

  await storeContractTest(
    async () => {
      dynamo = new Dynamo(client, randomUUID());
      await dynamo.createTable();
      return dynamo;
    },
    async () => {
      await dynamo.deleteTable();
    },
  );
});
