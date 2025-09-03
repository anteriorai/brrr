import { suite, test } from "node:test";
import { Dynamo } from "./dynamo.ts";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { storeContractTest } from "../store.test.ts";
import { randomUUID } from "node:crypto";
import { ok, strictEqual } from "node:assert/strict";
import { env } from "node:process";

await suite(import.meta.filename, async () => {
  ok(env.AWS_DEFAULT_REGION);
  ok(env.AWS_ENDPOINT_URL);

  const client = new DynamoDBClient({
    region: env.AWS_DEFAULT_REGION,
    endpoint: env.AWS_ENDPOINT_URL,
  });

  let dynamo: Dynamo;

  await test("NOCOOMIT MUST FAIL", async () => {
    strictEqual(true, false);
  });

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
