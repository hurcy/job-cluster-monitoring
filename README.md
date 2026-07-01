# Job Cluster Monitoring

Job & Cluster 워크로드 모니터링(**Right-Sizing** + **Disk-Spill**) 대시보드를 Databricks Asset Bundles(DAB)로 배포합니다.
**Serverless·SDP(Lakeflow Declarative Pipeline) 미사용** — 모든 분석은 classic 클러스터 노트북이 `system.*` 테이블을 **직접** 조회해 생성하며, 시스템 테이블 사본이 필요 없습니다.

## Preview
![alt text](https://github.com/hurcy/job-cluster-monitoring/blob/main/dashboard_example.png)

## 프로젝트 구조

```
job-cluster-monitoring/
├── databricks.yml                  # DAB 번들 설정 (변수, 타겟 정의)
├── build_dashboard.py              # 대시보드 리타깃 툴 (로컬 CLI ↔ 워크스페이스 노트북 양용)
├── resources/
│   ├── jobs.yml                    # 잡 리소스 (classic 클러스터 노트북 — serverless·SDP 미사용)
│   └── dashboard.yml               # AI/BI 대시보드 리소스
├── workload_analysis/
│   ├── ingestion/                  # 셋업 노트북 (classic 클러스터, system.* 직접 조회)
│   │   ├── workload_analysis_setup.py     # Right-Sizing 분석 테이블 생성 (system.* 직접)
│   │   └── spill_audit_setup.py           # Disk-Spill 뷰/테이블 + EXPANDED_DISK 이벤트 적재
│   ├── explorations/               # 탐색용 노트북
│   └── validation/                 # 데이터 품질 검증 쿼리
└── dashboard/
    └── *.lvdash.json               # 대시보드 정의 (Right-Sizing + Disk Spill 페이지)
```

## 사전 요구사항

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.218+
- 대상 워크스페이스에 대한 인증 프로파일 설정 (`~/.databrickscfg`)
- Unity Catalog 및 시스템 테이블 접근 권한

## 배포 방법

### 1. 인증 프로파일 설정

`~/.databrickscfg` 에 대상 워크스페이스 프로파일을 추가합니다.

```ini
[DEFAULT]
host  = https://adb-984752964297111.11.azuredatabricks.net
token = dapi...

[WORKSPACE_B]
host  = https://adb-xxxxxxxxxxxx.xx.azuredatabricks.net
token = dapi...
```

### 2. 번들 변수 확인

`databricks.yml`에 정의된 변수를 대상 워크스페이스에 맞게 조정합니다.

| 변수 | 설명 | 예시 |
|------|------|------|
| `catalog` | Unity Catalog 카탈로그 이름 (분석 결과 출력) | `hurcy` |
| `analytics_schema` | 분석 결과 출력 스키마 (Spill 뷰 + Right-Sizing 테이블) | `test` |
| `workspace_id` | 분석 대상 워크스페이스 ID | `984752964297111` |
| `warehouse_id` | 대시보드용 SQL Warehouse | `Shared Endpoint` (lookup) |

### 3. 새 타겟 추가 (다른 워크스페이스 배포)

`databricks.yml`의 `targets` 섹션에 새 타겟을 추가합니다.

```yaml
targets:
  dev:
    default: true
    mode: development
    workspace:
      host: https://adb-984752964297111.11.azuredatabricks.net
      profile: DEFAULT

  # 다른 워크스페이스 예시
  prod:
    workspace:
      host: https://adb-xxxxxxxxxxxx.xx.azuredatabricks.net
      profile: WORKSPACE_B
    variables:
      catalog: "my_catalog"
      analytics_schema: "my_schema"
      workspace_id: "xxxxxxxxxxxx"
      warehouse_id: "xxxxxxxxxxxxxxxx"
```

### 4. 배포 실행

```bash
# 기본 타겟(dev) 배포
databricks bundle deploy

# 특정 타겟 배포
databricks bundle deploy -t prod

# 변수 오버라이드
databricks bundle deploy -t prod \
  --var="catalog:other_catalog" \
  --var="workspace_id:123456789"
```

> **다른 catalog.schema 로 배포 시** — `analytics_schema`/`catalog` 변수를 바꾼 뒤 대시보드도
> 같은 타깃으로 재생성하세요: `CATALOG=<c> SCHEMA=<s> OUT=dashboard/<name>.lvdash.json python3 build_dashboard.py`

### 5. 분석 잡 실행 (serverless·SDP 미사용)

```bash
# Right-Sizing 테이블 + Disk-Spill 뷰/이벤트를 한 잡(두 태스크)으로 생성/갱신
# — classic 클러스터에서 system.* 직접 조회
databricks bundle run workload_monitoring_refresh -t <target>
```

> **Right-Sizing 임계치 조정:** sizing 판정 임계치(min_runs, downsize/likely CPU·Mem,
> upsize P95, swap, spill, iowait)는 `workload_analysis_setup` 태스크의 **notebook 파라미터**로
> 노출됩니다. 기본값은 `resources/jobs.yml`의 `base_parameters`에, 런타임 조정은 Jobs UI의
> *Run now with different parameters* 로 합니다.

> **Right-Sizing 전제조건:** `system.compute.clusters` 가 조회 가능해야 합니다. 일부 워크스페이스에서
> 미프로비저닝(`UC_DEPENDENCY_DOES_NOT_EXIST`)일 수 있으니 사전 확인하세요. Disk-Spill 은
> `system.query.history` / `system.compute.node_timeline` 만 사용해 해당 제약이 없습니다.

### 6. 대시보드

대시보드는 Global Filters를 통해 **카탈로그**, **스키마**, **조회 기간**을 런타임에 변경할 수 있습니다.
출력 catalog/schema 가 바뀌면 `build_dashboard.py` 로 대시보드를 재생성하거나 `p_catalog`/`p_schema` 기본값을 함께 변경하세요.

### 7. 배포 상태 확인 / 삭제

```bash
# 배포 상태 확인
databricks bundle validate -t prod

# 리소스 삭제
databricks bundle destroy -t prod
```

## Disk-Spill / Expanded-Disk 감사

Job Compute · Serverless · SQL Warehouse 에서 실행된 코드/쿼리의 **로컬 디스크 spill** 과
**elastic-disk 확장 압력**을 분석하여 대시보드의 **Disk Spill** / **Spill Detail & Expanded Disk**
페이지로 노출한다. (출처: `spill_audit_notebook.py`)

**Serverless 를 쓸 수 없는 Pro SQL Warehouse 환경**에 맞춰, spill 분석 계층은 DLT MV 가 아닌
**일반 SQL VIEW** 로 구현한다 — Pro warehouse 가 system table 을 직접 조회하므로
serverless · DLT · 시스템테이블 사본이 전혀 필요 없다. **분석 윈도우는 최근 30일.**

| 신호 | 소스 (직접 조회) | 산출물 | 런타임 |
|------|------|--------|------|
| Method A — 문장 단위 spill | `system.query.history` | `query_spill_v` (compute type · job 귀속 · 근본원인 태그) | Pro SQL Warehouse |
| Method B — 클러스터 disk-free floor / swap | `system.compute.node_timeline` | `cluster_disk_pressure_v` (확장 신호, non-SQL 잡 포함) | Pro SQL Warehouse |
| 실제 확장 이벤트 | Cluster Events API (`/api/2.0/clusters/events`) | `expanded_disk_events` Delta 테이블 (durable log) | Classic single-node 클러스터 |

**근본원인 태그** (`query_spill_v.root_cause`): `WIDE_SHUFFLE` · `NO_PRUNING` ·
`MV_REFRESH` · `MEMORY_BOUND` · `MODERATE` — 각각 처방을 `root_cause_detail` 에 담는다.

### 변수-구동 배포 (임의 카탈로그.스키마 / 워크스페이스)

뷰·테이블·이벤트 적재는 `spill_audit_setup.py` 노트북 한 개가 **번들 변수**로 전부 파라미터화해
생성한다(`CREATE VIEW` 가 파라미터 마커를 못 받아 Python f-string 으로 주입). **per-customer SQL
수정 불필요.** 고객사 배포 절차:

1. `databricks.yml` 변수 설정: `catalog`, `analytics_schema`, `workspace_id`, `warehouse_id`.
2. 대시보드를 같은 타깃으로 재생성 (`p_catalog`/`p_schema` 기본값 + 카탈로그/스키마 글로벌 필터 + 드라이버 dataset 일괄 치환). 두 가지 방법:
   - **로컬 CLI** (배포 전): `python3 build_dashboard.py` — `databricks.yml` 의 `catalog`/`analytics_schema` 를 읽어 `.lvdash.json` 을 재생성. (`CATALOG=`/`SCHEMA=`/`OUT=` 로 오버라이드 가능)
   - **워크스페이스 노트북** (배포 후): `build_dashboard.py` 를 노트북으로 열어 `catalog`/`schema`/`dashboard_id`(+선택 `publish_warehouse_id`) 위젯 설정 후 실행 → 배포된 Lakeview 대시보드를 Lakeview API 로 직접 리타깃·게시.
3. `databricks bundle deploy -t <target>` → `databricks bundle run workload_monitoring_refresh -t <target>`.

> **전제조건(고객 워크스페이스):** `query`/`compute`/`lakeflow` **system schema 활성화**,
> Pro/Serverless **SQL Warehouse** 1개, 타깃 스키마에 `CREATE` 권한 + system tables `SELECT` 권한.
> system tables 는 멀티-워크스페이스이므로 `workspace_id` 필터 필수.

> **EXPANDED_DISK 이벤트 주의:** 실제 확장 이벤트는 system table 이 아닌 Cluster Events API 에만
> 있고(=SQL 로 호출 불가, 노트북+컴퓨트 필요), **종료된 job 클러스터는 ~30일 후 purge** 되어
> 조회 불가. Serverless 는 이벤트가 없다. 그래서 노트북이 **매일** 이벤트를 Delta 로 스냅샷해
> API 보존 윈도우를 넘어서는 **영속 이벤트 로그**를 쌓는다 — 초기에는 비어 있을 수 있으며 시간이
> 지나며 채워진다. 모든 클러스터의 즉시 신호는 `cluster_disk_pressure_v`(disk-free floor) 가 커버한다.

병합된 **`Workload Monitoring Refresh`** 잡(`resources/jobs.yml`)의 `spill_audit_setup` 태스크가
매일 실행된다 — `spill_audit_setup` 노트북이 **classic single-node 클러스터**(serverless 미사용)에서
뷰/테이블 생성 후 이벤트를 적재. (같은 잡의 `workload_analysis_setup` 태스크는 Right-Sizing 테이블 생성.)

## License
MIT License
