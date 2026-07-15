# Job Cluster Monitoring

Job & Cluster 워크로드 모니터링(**Right-Sizing** + **Disk-Spill**) 대시보드를 Databricks Asset Bundles(DAB)로 배포합니다.
**Serverless·SDP(Lakeflow Declarative Pipeline) 미사용** — 모든 분석은 classic 클러스터 노트북이 `system.*` 테이블을 **직접** 조회해 생성하며, 시스템 테이블 사본이 필요 없습니다.

## Preview
![alt text](https://github.com/hurcy/job-cluster-monitoring/blob/main/dashboard_example.png)

## Genie Code Skills (co-pilot)

대시보드에 더해, 이 repo는 분석 쿼리·라이프사이클을 **Databricks Genie Code Skills**로 제공합니다.
[Genie Code](https://docs.databricks.com/aws/en/genie-code/skills)(워크스페이스 네이티브 AI 어시스턴트)에
skill로 등록하면, 워크스페이스 사용자·관리자가 대화형으로 컴퓨트 최적화를 수행할 수 있습니다 —
예: *"어떤 클러스터가 오버사이징이고 얼마 절감 가능?"*, *"뭐가 spill 나고 왜?"*, *"이번 달 top 비용 잡은?"*.

`skills/` 아래 6개 skill (`SKILL.md` = Claude Code와 동일한 오픈 Agent Skills 표준):

| Skill | 역할 |
|------|------|
| `job-cluster-monitoring` | 엄브렐러 라우터 — 테이블 카탈로그, 공유 출력 계약, 질문 라우팅 |
| `job-monitoring-setup` | 파이프라인 배포·리프레시·UC 주석 재시딩 (유일하게 쓰기) |
| `compute-right-sizing` | 오버/언더 프로비저닝 진단 + 추정 절감 (`right_sizing_analysis_mv` 등) |
| `disk-spill-diagnosis` | spill 근본원인·disk pressure·EXPANDED_DISK (`query_spill_v` 등) |
| `compute-cost-analysis` | DBU→USD, classic vs serverless, top spenders (`job_run_cost_analysis_mv`) |
| `monitoring-data-quality` | 집계 리컨실리에이션 검증 (T01–T17) |

- **End-to-end 사용 시나리오**: [`SCENARIO.md`](SCENARIO.md) — 관리자가
  Genie Code로 셋업→비용→right-sizing→spill→검증을 자연어로 수행하는 실측 기반 워크스루.
- **분석 skill은 read-only(SELECT)** — 셋업 skill이 만든 테이블을 질의만 합니다.
- 추천은 **Problem / Action / $ Savings / Effort / Risk / Docs** 계약을 따라
  [`fe-cost-optimization-report`](https://github.com/databricks-field-eng/vibe)의 근거 계층과 맞물립니다.
  (그쪽은 내부 `centralized_system_tables` 미러, 이쪽은 고객 워크스페이스 live `system.*` — 보완 관계.)

### 발행 (publish)

```bash
# 파이프라인(잡+대시보드) 배포 AND skills 를 /Workspace/.assistant/skills 로 발행
make deploy                                   # = bundle deploy + workspace import-dir skills

# skill 만 발행 (개인 폴더로)
make skills SKILLS_ROOT=/Users/<you>/.assistant/skills
```

> DAB 에는 skill 리소스 타입이 없어 `bundle deploy` 로는 `.assistant/skills` 경로에 파일을 놓을 수 없습니다
> (sync 는 번들 루트로만 감). 그래서 `databricks.yml` 은 `sync.exclude: ["skills/**"]` 로 두고,
> skill 은 `databricks workspace import-dir` 로 발행하며 `make deploy` 가 둘을 한 번에 묶습니다.
> Genie Code 에서 skill 편집 후에는 **새 대화 스레드**로 시작하면 반영됩니다.

## 프로젝트 구조

```
job-cluster-monitoring/
├── Makefile                        # make deploy (bundle + skills 발행) / make refresh
├── databricks.yml                  # DAB 번들 설정 (변수, 타겟, skills/ sync 제외)
├── build_dashboard.py              # 배포된 대시보드 리타깃 (export 로 읽기 → Lakeview PATCH 로 쓰기)
├── skills/                         # Genie Code Skills (→ /Workspace/.assistant/skills)
│   ├── job-cluster-monitoring/     # 엄브렐러 라우터 + 공유 출력 계약
│   ├── job-monitoring-setup/       # 배포·리프레시·주석 재시딩 (유일하게 쓰기)
│   ├── compute-right-sizing/       # 오버/언더 프로비저닝 진단 + 절감 (queries/ + references/)
│   ├── disk-spill-diagnosis/       # spill·disk pressure·EXPANDED_DISK (queries/ + references/)
│   ├── compute-cost-analysis/      # DBU→USD, classic vs serverless, top spenders (queries/)
│   └── monitoring-data-quality/    # 리컨실리에이션 검증 (T01–T17)
├── resources/
│   ├── jobs.yml                    # 잡 리소스 (setup 2 태스크; 주석은 CREATE에 인라인)
│   └── dashboard.yml               # AI/BI 대시보드 리소스
├── workload_analysis/
│   ├── ingestion/                  # 셋업 노트북 (classic 클러스터, system.* 직접 조회)
│   │   ├── workload_analysis_setup.py     # Right-Sizing 테이블 생성 (주석 인라인/co-located)
│   │   └── spill_audit_setup.py           # Disk-Spill 뷰/테이블 + EXPANDED_DISK (주석 인라인)
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

> **다른 catalog.schema 로 배포 시** — `analytics_schema`/`catalog` 변수를 바꿔 배포한 뒤,
> **배포된 대시보드를** `python3 build_dashboard.py` 로 리타깃하세요 (아래 6번 참조).

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

### 6. 대시보드 리타깃 (`build_dashboard.py`)

대시보드는 Global Filters를 통해 **카탈로그**, **스키마**, **조회 기간**을 런타임에 변경할 수 있습니다.
Disk-Spill 페이지 추가와 catalog/schema 리타깃은 **배포된 대시보드를 직접 수정**하는 방식입니다 —
`databricks bundle deploy` **후에** 실행하세요:

```bash
# 배포된 대시보드를 Workspace Export API 로 읽고 → 리타깃 → Lakeview PATCH API 로 다시 씀
python3 build_dashboard.py
```

- catalog/schema 는 `databricks.yml` 에서 읽습니다 (`CATALOG=`/`SCHEMA=` 로 오버라이드).
- 대상 대시보드는 표시 이름 `[<target>] Job Cluster Monitoring Dashboard` 로 찾습니다
  (`DASHBOARD_ID`/`DASHBOARD_NAME`/`TARGET` 로 오버라이드, 인증은 SDK 표준 — bundle 타깃 profile).
- **로컬 `.lvdash.json` 을 수정하지 않습니다.** (idempotent — 여러 번 실행해도 안전)

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
2. `databricks bundle deploy -t <target>` — 대시보드·뷰·테이블·잡 배포 (대시보드는 로컬 `.lvdash.json` 기준으로 생성).
3. `python3 build_dashboard.py` — **배포된** 대시보드를 Workspace Export API 로 읽어 Disk-Spill 페이지 추가 + `p_catalog`/`p_schema` 기본값·글로벌 필터·드라이버 dataset 을 catalog/schema 로 일괄 치환한 뒤 **Lakeview PATCH API 로 반영**. (`CATALOG=`/`SCHEMA=`/`DASHBOARD_ID`/`TARGET` 오버라이드 가능, idempotent)
4. `databricks bundle run workload_monitoring_refresh -t <target>` — 분석 잡 실행.

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
