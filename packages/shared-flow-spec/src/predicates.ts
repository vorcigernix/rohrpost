import type { PredicateExpr } from "./types";

const UNSAFE_PATH_SEGMENTS = new Set(["__proto__", "constructor", "prototype"]);

function hasUnsafeSegment(parts: string[]): boolean {
  return parts.some((part) => UNSAFE_PATH_SEGMENTS.has(part));
}

function getPathValue(input: unknown, path: string): unknown {
  if (!path) return undefined;
  const parts = path.split(".");
  let current: any = input;

  for (const part of parts) {
    if (current == null) return undefined;
    current = current[part];
  }

  return current;
}

export function evaluatePredicate(expr: PredicateExpr, payload: unknown): boolean {
  switch (expr.type) {
    case "always":
      return true;
    case "field_exists":
      return getPathValue(payload, expr.path) !== undefined;
    case "field_equals":
      return Object.is(getPathValue(payload, expr.path), expr.value);
    case "field_contains": {
      const value = getPathValue(payload, expr.path);
      return typeof value === "string" && value.includes(expr.value);
    }
    case "field_gt":
      return Number(getPathValue(payload, expr.path)) > expr.value;
    case "field_gte":
      return Number(getPathValue(payload, expr.path)) >= expr.value;
    case "field_lt":
      return Number(getPathValue(payload, expr.path)) < expr.value;
    case "field_lte":
      return Number(getPathValue(payload, expr.path)) <= expr.value;
    case "and":
      return expr.all.every((predicate) => evaluatePredicate(predicate, payload));
    case "or":
      return expr.any.some((predicate) => evaluatePredicate(predicate, payload));
    case "not":
      return !evaluatePredicate(expr.predicate, payload);
  }
}

export function getPath(input: unknown, path: string): unknown {
  return getPathValue(input, path);
}

export function setPath(input: unknown, path: string, value: unknown): unknown {
  if (!path) return input;
  const parts = path.split(".");
  if (hasUnsafeSegment(parts)) {
    return input;
  }

  const root =
    input && typeof input === "object" && !Array.isArray(input)
      ? (structuredClone(input) as Record<string, unknown>)
      : {};

  let current: Record<string, unknown> = root;

  for (let index = 0; index < parts.length - 1; index += 1) {
    const key = parts[index];
    const existing = current[key];
    if (existing && typeof existing === "object" && !Array.isArray(existing)) {
      current = existing as Record<string, unknown>;
    } else {
      const next: Record<string, unknown> = {};
      current[key] = next;
      current = next;
    }
  }

  current[parts[parts.length - 1]] = value;
  return root;
}

export function deletePath(input: unknown, path: string): unknown {
  if (!path) return input;
  const parts = path.split(".");
  if (hasUnsafeSegment(parts)) {
    return input;
  }

  const root =
    input && typeof input === "object" && !Array.isArray(input)
      ? (structuredClone(input) as Record<string, unknown>)
      : {};

  let current: Record<string, unknown> | undefined = root;

  for (let index = 0; index < parts.length - 1; index += 1) {
    const key = parts[index];
    const next = current?.[key];
    if (!next || typeof next !== "object" || Array.isArray(next)) {
      return root;
    }
    current = next as Record<string, unknown>;
  }

  if (current) {
    delete current[parts[parts.length - 1]];
  }

  return root;
}
