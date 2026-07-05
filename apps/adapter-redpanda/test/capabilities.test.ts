import { describe, expect, it } from "bun:test";

import {
  buildConnectorManifests,
  findManifest,
  findManifestForConnectorRef,
} from "../src/manifests";

describe("adapter manifests", () => {
  it("exposes adapter-owned manifests for stream and warehouse sinks", () => {
    const manifests = buildConnectorManifests("example/connect:latest");

    expect(manifests).toHaveLength(5);
    expect(findManifest(manifests, "kafka-source")?.executionMode).toBe("adapter");
    expect(findManifestForConnectorRef(manifests, "snowflake_sink")?.id).toBe("snowflake-sink");
  });
});
