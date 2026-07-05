import { describe, expect, test } from "bun:test";
import { deletePath, getPath, setPath } from "../src/predicates";

describe("setPath", () => {
  test("sets nested paths, creating intermediate objects", () => {
    expect(setPath({ a: 1 }, "user.ssn", "[redacted]")).toEqual({ a: 1, user: { ssn: "[redacted]" } });
  });

  test("overwrites existing values without mutating the input", () => {
    const input = { user: { ssn: "123" } };
    const result = setPath(input, "user.ssn", "[redacted]");
    expect(result).toEqual({ user: { ssn: "[redacted]" } });
    expect(input.user.ssn).toBe("123");
  });

  test("refuses to write through __proto__, constructor, or prototype segments", () => {
    const before = Object.keys(Object.prototype);
    expect(setPath({ a: 1 }, "__proto__.polluted", "X")).toEqual({ a: 1 });
    expect(setPath({ a: 1 }, "constructor.prototype.polluted", "X")).toEqual({ a: 1 });
    expect(setPath({ a: 1 }, "prototype.polluted", "X")).toEqual({ a: 1 });
    expect(Object.keys(Object.prototype)).toEqual(before);
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
  });
});

describe("deletePath", () => {
  test("deletes a nested path without mutating the input", () => {
    const input = { user: { ssn: "123", name: "Ada" } };
    expect(deletePath(input, "user.ssn")).toEqual({ user: { name: "Ada" } });
    expect(input.user.ssn).toBe("123");
  });

  test("refuses to traverse __proto__, constructor, or prototype segments", () => {
    expect(deletePath({ a: 1 }, "__proto__.toString")).toEqual({ a: 1 });
    expect(typeof {}.toString).toBe("function");
  });
});

describe("getPath", () => {
  test("reads nested paths and returns undefined for missing ones", () => {
    expect(getPath({ user: { ssn: "123" } }, "user.ssn")).toBe("123");
    expect(getPath({ user: {} }, "user.ssn")).toBeUndefined();
  });
});
