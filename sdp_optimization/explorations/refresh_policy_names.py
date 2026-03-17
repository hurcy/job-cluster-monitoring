"""
Databricks SDK로 클러스터 정책 목록을 가져와
hurcy.default.cluster_policy_names 테이블에 저장한다.

사용법:
  python refresh_policy_names.py                    # DEFAULT 프로파일 사용
  python refresh_policy_names.py --profile myprof   # 특정 프로파일
  python refresh_policy_names.py --warehouse-id ID  # 특정 warehouse
"""

import argparse
import json
import subprocess
import sys

from databricks.sdk import WorkspaceClient


def get_policies(profile: str | None = None) -> list[dict]:
    """Databricks SDK로 클러스터 정책 목록 조회."""
    kwargs = {"profile": profile} if profile else {}
    w = WorkspaceClient(**kwargs)
    policies = []
    for p in w.cluster_policies.list():
        policies.append({
            "policy_id": p.policy_id,
            "policy_name": p.name,
        })
    return policies


def execute_sql(statement: str, warehouse_id: str, profile: str | None = None) -> dict:
    """SQL Statement Execution API로 SQL 실행."""
    payload = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "30s",
    }
    cmd = [
        "databricks", "api", "post", "/api/2.0/sql/statements",
        "--json", json.dumps(payload),
    ]
    if profile:
        cmd += ["--profile", profile]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return json.loads(result.stdout)


def refresh_table(
    policies: list[dict],
    warehouse_id: str,
    catalog: str = "hurcy",
    schema: str = "default",
    profile: str | None = None,
) -> None:
    """cluster_policy_names 테이블을 DROP → CREATE → INSERT로 갱신."""
    table = f"{catalog}.{schema}.cluster_policy_names"

    # DROP + CREATE
    for stmt in [
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} (policy_id STRING, policy_name STRING) USING DELTA",
    ]:
        resp = execute_sql(stmt, warehouse_id, profile)
        state = resp.get("status", {}).get("state", "UNKNOWN")
        if state != "SUCCEEDED":
            err = resp.get("status", {}).get("error", {}).get("message", "")
            print(f"FAILED: {stmt[:60]}... → {err}", file=sys.stderr)
            sys.exit(1)

    # INSERT in batches
    batch_size = 50
    for i in range(0, len(policies), batch_size):
        batch = policies[i : i + batch_size]
        values = ", ".join(
            f"('{p['policy_id'].replace(chr(39), chr(39)*2)}', "
            f"'{p['policy_name'].replace(chr(39), chr(39)*2)}')"
            for p in batch
        )
        resp = execute_sql(f"INSERT INTO {table} VALUES {values}", warehouse_id, profile)
        state = resp.get("status", {}).get("state", "UNKNOWN")
        if state != "SUCCEEDED":
            err = resp.get("status", {}).get("error", {}).get("message", "")
            print(f"FAILED insert batch {i // batch_size + 1}: {err}", file=sys.stderr)
            sys.exit(1)

    print(f"OK: {len(policies)} policies → {table}")


def main():
    parser = argparse.ArgumentParser(description="Refresh cluster_policy_names table")
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--warehouse-id", default="148ccb90800933a1")
    parser.add_argument("--catalog", default="hurcy")
    parser.add_argument("--schema", default="default")
    args = parser.parse_args()

    print(f"Fetching policies (profile={args.profile})...")
    policies = get_policies(args.profile)
    print(f"Found {len(policies)} policies")

    print(f"Writing to {args.catalog}.{args.schema}.cluster_policy_names...")
    refresh_table(policies, args.warehouse_id, args.catalog, args.schema, args.profile)


if __name__ == "__main__":
    main()
