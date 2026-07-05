import { describe, expect, test } from "bun:test";
import {
  classifyNatsPublishError,
  createJetStreamConsumerOptions,
  isConsumerConfigDrifted,
  isConsumerConflictError,
  selectStaleJetStreamConsumers,
} from "../nats";

describe("classifyNatsPublishError", () => {
  test("maps JetStream storage exhaustion to a client-facing 503", () => {
    const error = Object.assign(new Error("503"), {
      code: "503",
      api_error: {
        code: 503,
        err_code: 10023,
        description: "insufficient resources",
      },
    });

    const response = classifyNatsPublishError(error);

    expect(response).not.toBeNull();
    expect(response?.status).toBe(503);
    expect(response?.body.error).toBe("jetstream_backpressure");
    expect(response?.body.apiError?.errCode).toBe(10023);
  });

  test("maps JetStream discard-new capacity rejection to a client-facing 503", () => {
    const error = Object.assign(new Error("503"), {
      code: "503",
      api_error: {
        code: 503,
        err_code: 10077,
        description: "maximum bytes exceeded",
      },
    });

    const response = classifyNatsPublishError(error);

    expect(response).not.toBeNull();
    expect(response?.status).toBe(503);
    expect(response?.body.error).toBe("jetstream_backpressure");
  });

  test("maps other JetStream API errors to a 502", () => {
    const error = Object.assign(new Error("503"), {
      code: "503",
      api_error: {
        code: 503,
        err_code: 12345,
        description: "some other broker-side failure",
      },
    });

    const response = classifyNatsPublishError(error);

    expect(response).not.toBeNull();
    expect(response?.status).toBe(502);
    expect(response?.body.error).toBe("jetstream_publish_failed");
  });

  test("ignores non-NATS errors", () => {
    expect(classifyNatsPublishError(new Error("plain error"))).toBeNull();
    expect(classifyNatsPublishError("boom")).toBeNull();
  });
});

describe("createJetStreamConsumerOptions", () => {
  test("sanitizes durable names and uses deliver-all semantics for workqueue streams", () => {
    const options = createJetStreamConsumerOptions("router.ingress.tenant-a.flow-http.rev-http-v1.>");
    const config = (options as unknown as {
      config: {
        deliver_policy?: string;
        ack_policy?: string;
        durable_name?: string;
        deliver_subject?: string;
        ack_wait?: number;
        max_ack_pending?: number;
      };
    }).config;

    expect(config.deliver_policy).toBe("all");
    expect(config.ack_policy).toBe("explicit");
    expect(config.durable_name).toBe("router_ingress_tenant-a_flow-http_rev-http-v1__");
    expect(config.deliver_subject).toStartWith("_INBOX.");
    expect(config.ack_wait).toBe(30_000 * 1_000_000);
    expect(config.max_ack_pending).toBe(64);
  });
});

describe("selectStaleJetStreamConsumers", () => {
  test("selects only unbound consumers that block the same workqueue filter", () => {
    const stale = selectStaleJetStreamConsumers(
      [
        {
          name: "deploy_old_router_ingress_tenant_demo_flow_demo_orders_rev_demo_orders_v1__",
          config: {
            filter_subject: "router.ingress.tenant_demo.flow_demo_orders.rev_demo_orders_v1.>",
          },
        },
        {
          name: "router_ingress_tenant_demo_flow_demo_orders_rev_demo_orders_v1__",
          push_bound: true,
          config: {
            filter_subject: "router.ingress.tenant_demo.flow_demo_orders.rev_demo_orders_v1.>",
          },
        },
        {
          name: "other_consumer",
          config: {
            filter_subject: "router.ingress.tenant_demo.flow_other.rev_other_v1.>",
          },
        },
      ],
      "router.ingress.tenant_demo.flow_demo_orders.rev_demo_orders_v1.>",
      "router.ingress.tenant_demo.flow_demo_orders.rev_demo_orders_v1.>",
    );

    expect(stale).toEqual([
      "deploy_old_router_ingress_tenant_demo_flow_demo_orders_rev_demo_orders_v1__",
    ]);
  });

  test("selects the same-name consumer when its ack config drifted", () => {
    const subject = "router.ingress.tenant_demo.flow_demo_orders.rev_demo_orders_v1.>";
    const durable = "router_ingress_tenant_demo_flow_demo_orders_rev_demo_orders_v1__";

    const drifted = selectStaleJetStreamConsumers(
      [{ name: durable, config: { filter_subject: subject, ack_wait: 5_000 * 1_000_000, max_ack_pending: 10 } }],
      subject,
      subject,
    );
    expect(drifted).toEqual([durable]);

    const matching = selectStaleJetStreamConsumers(
      [{ name: durable, config: { filter_subject: subject, ack_wait: 30_000 * 1_000_000, max_ack_pending: 64 } }],
      subject,
      subject,
    );
    expect(matching).toEqual([]);
  });

  test("never selects a push-bound same-name consumer even when drifted", () => {
    const subject = "router.ingress.tenant_demo.flow_demo_orders.rev_demo_orders_v1.>";
    const durable = "router_ingress_tenant_demo_flow_demo_orders_rev_demo_orders_v1__";
    expect(
      selectStaleJetStreamConsumers(
        [{ name: durable, push_bound: true, config: { filter_subject: subject, ack_wait: 1, max_ack_pending: 1 } }],
        subject,
        durable,
      ),
    ).toEqual([]);
  });
});

describe("isConsumerConfigDrifted", () => {
  test("matching config is not drifted", () => {
    expect(isConsumerConfigDrifted({ ack_wait: 30_000 * 1_000_000, max_ack_pending: 64 })).toBe(false);
  });

  test("single-field drift is detected", () => {
    expect(isConsumerConfigDrifted({ ack_wait: 5_000 * 1_000_000, max_ack_pending: 256 })).toBe(true);
    expect(isConsumerConfigDrifted({ ack_wait: 30_000 * 1_000_000, max_ack_pending: 1_000 })).toBe(true);
  });

  test("legacy consumers with unset limits are drifted", () => {
    expect(isConsumerConfigDrifted({})).toBe(true);
    expect(isConsumerConfigDrifted(undefined)).toBe(true);
  });
});

describe("isConsumerConflictError", () => {
  test("matches workqueue filtered-consumer conflicts by err code", () => {
    const error = Object.assign(new Error("consumers filter subjects overlap"), {
      code: "503",
      api_error: { code: 400, err_code: 10100, description: "consumers filter subjects overlap" },
    });
    expect(isConsumerConflictError(error)).toBe(true);
  });

  test("matches duplicate subscription errors by message", () => {
    expect(isConsumerConflictError(new Error("duplicate subscription"))).toBe(true);
  });

  test("does not match transient errors", () => {
    expect(isConsumerConflictError(new Error("timeout"))).toBe(false);
    expect(isConsumerConflictError(undefined)).toBe(false);
  });
});
