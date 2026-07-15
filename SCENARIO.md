# End-to-End 시나리오 — Genie Code 컴퓨트 최적화 co-pilot

이 문서는 이번 배포로 워크스페이스에 등록된 **6개 Genie Code Skill**을, 관리자가
Databricks 워크스페이스 안에서 자연어로 사용하는 흐름을 처음부터 끝까지 보여준다.
모든 수치는 실제 `e2-demo-field-eng` 워크스페이스(`main.cinyoung_hur_monitoring`)에
배포·실행해 얻은 값이다.

**등장 skill**: `job-cluster-monitoring`(라우터) · `job-monitoring-setup` · `compute-cost-analysis` ·
`compute-right-sizing` · `disk-spill-diagnosis` · `monitoring-data-quality`.

**페르소나**: *지수* — 플랫폼 관리자. "지난 분기 대비 Databricks 컴퓨트 비용이 올랐다,
어디서 새는지 찾고 줄여라"는 요청을 받았다. 워크스페이스의 **Genie Code** 패널을 co-pilot으로 쓴다.

> 전제: 관리자가 이 repo를 `make deploy TARGET=<ws>`로 배포해 6개 skill이
> `/Workspace/.assistant/skills/`(또는 개인 폴더)에 등록돼 있다. 분석 skill은 모두 **read-only**.

---

## Act 0 — 셋업: "모니터링 테이블부터 만들어줘"

지수가 Genie Code 새 대화에 입력:

> **"이 워크스페이스 잡·클러스터 모니터링 세팅해줘. 적절한 사이징이랑 스필 분석하게."**

- **로드되는 skill**: `job-cluster-monitoring`(라우터) → `job-monitoring-setup`.
- **뒤에서**: skill이 전제조건을 확인(`SELECT 1 FROM system.compute.node_timeline`,
  `system.compute.clusters` 프로비저닝 여부)하고, `make deploy` → `make refresh` 절차를 안내·수행.
  `workload_monitoring_refresh` 잡이 **classic 클러스터에서 `system.*`를 직접 조회**해
  9개 테이블/뷰를 `catalog.schema`에 생성. 테이블/컬럼 코멘트는 CREATE 문에 인라인 선언돼
  Genie 메타데이터로 바로 쓰인다.
- **결과 (실측)**: `instance_workload_analysis_mv` 318,001행 · `job_run_cost_analysis_mv` 302,131행 ·
  `right_sizing_analysis_mv` 3,585행 · `query_spill_v` 18,013행 · `cluster_disk_pressure_v` 100,802행.

> 만약 어떤 분석 skill이 "table not found"를 만나면, 라우터가 자동으로 이 skill로 보내 셋업을 먼저 유도한다.

---

## Act 1 — 큰 그림: "돈이 어디로 나가?"

> **"지난 90일 컴퓨트 비용, classic이랑 serverless로 나눠서 보여줘. 제일 비싼 잡도."**

- **로드되는 skill**: `compute-cost-analysis`.
- **뒤에서**: `queries/cost_by_compute_type.sql`, `queries/top_spenders.sql`를
  `job_run_cost_analysis_mv`에 대해 실행(`{{catalog}}.{{schema}}` 치환).
- **Genie 응답 (실측)**:
  - 총 컴퓨트 **$191,077 / 90일** — Classic **$141,394 (74%)**, Serverless **$49,683 (26%)**.
  - "Classic이 4분의 3을 차지합니다. Classic은 리사이징 여지가 있으니 거기부터 보시죠."
- **다음 행동 유도**: "Classic 비용의 상당 부분이 저활용 클러스터일 수 있습니다 —
  right-sizing으로 넘어갈까요?" → 지수: "응."

---

## Act 2 — 급소: "오버사이징 클러스터 찾고 절감액 알려줘"

> **"어떤 클러스터가 과하게 잡혀 있어? 줄이면 얼마 아껴?"**

- **로드되는 skill**: `compute-right-sizing`.
- **뒤에서**: `queries/top_downsize_candidates.sql`(DEFINITE/LIKELY_DOWNSIZE를 절감액 순),
  `queries/savings_by_owner.sql`를 `right_sizing_analysis_mv`에 실행. `sizing_recommendation`
  taxonomy와 임계값은 `references/sizing-taxonomy.md`로 해석.
- **Genie 응답 — 출력 계약(Problem/Action/$Savings/Effort/Risk/Docs)** (실측):

  | # | Problem | Action | $ Savings(90d) | Effort | Risk |
  |---|---|---|---|---|---|
  | 1 | Harika의 All-Purpose 클러스터 `r5d.8xlarge × 27 워커`가 **평균 CPU 0.6%, 평균 메모리 4.2%** (2회 실행) — `DEFINITE_DOWNSIZE` | 워커 수 50% 감소 또는 더 작은 인스턴스 타입 | **$3,290** | S | L |
  | 2 | Rahul의 `g5.12xlarge × 4` GPU 클러스터 CPU 0.8% — `DEFINITE_DOWNSIZE` | 워커/타입 축소 (GPU 필요성 재검토) | **$1,476** | S | L |
  | 3 | `payments-gts-wib-rtm` `rd-fleet.4xlarge × 40` CPU 2.4% — `DEFINITE_DOWNSIZE` | 워커 수 축소 | **$1,458** | M | M |

  - **오너별 집중도** (어디부터 손댈지): harika **$6,581** · ketan **$3,985** · brandon **$3,348** ·
    zach $1,909 · rahul $1,476 · ameer $1,113.
  - 각 Action은 `docs.databricks.com`의 클러스터 사이징/오토스케일/정책 문서 URL로 뒷받침.
  - 한국어 `recommendation_detail`은 지수의 언어로 번역돼 Problem/Action에 반영.
- **가드레일**: 저활용이라도 `INSUFFICIENT_RUNS`/`NO_UTIL_DATA`는 "판단 불가"로 표시(과잉 권고 방지).
  All-Purpose 클러스터는 여러 잡이 공유하므로 "이 클러스터를 N개 잡이 함께 쓴다"를 함께 경고.

지수: "harika, ketan 두 명만 처리해도 분기 $10K 넘게 아끼네. 근데 저 payments 잡은 왜 이렇게 큰 걸 쓰지?"

---

## Act 3 — 원인 규명: "이 잡이 왜 크게 잡혀 있고 느려?"

> **"payments 관련 잡들 디스크 spill 나는지, 나면 왜 그런지 알려줘."**

- **로드되는 skill**: `disk-spill-diagnosis`.
- **뒤에서**: `queries/spill_root_cause_rollup.sql`, `queries/top_spilling_statements.sql`를
  `query_spill_v`에 실행. `root_cause`/`pressure_signal` taxonomy는 `references/spill-taxonomy.md`로 해석.
- **Genie 응답 (root_cause별 집계 — 실측, `main.cinyoung_hur_monitoring`)**:
  - `MV_REFRESH`가 spill 압도적 1위: **~71.8 TB** (9,980 statements, 55개 잡) — 전체 리컴퓨트 의심. Action: incremental refresh 검증.
  - `WIDE_SHUFFLE`: **~26.9 TB** (488 statements, statement당 평균 55 GB; SQL Warehouse는 평균 **135 GB**) — Action: AQE skew join, shuffle 파티션 상향, join/dedup 키로 Z-order.
  - `NO_PRUNING`: **~16.1 TB** (7,150 statements, 56개 잡) — 파일 프루닝 0인 풀스캔. Action: 파티션/클러스터링 predicate 추가.
  - `MEMORY_BOUND`: spill > input — Action: 메모리 최적화 노드 또는 파티션 축소.
- **핸드오프**: spill이 `MEMORY_BOUND`이고 클러스터에 swap이 있으면, "이건 오버사이징이 아니라
  **메모리 부족**입니다 — right-sizing에서 다운사이즈 대신 메모리 최적화 타입 권고로 연결" 하고
  `compute-right-sizing`의 `CONSIDER_UPSIZE` 신호(swap/spill/P95)와 교차 확인.
- **디스크 압박**: `queries/cluster_disk_pressure.sql`로 `DISK_PRESSURE`(로컬 디스크 여유 <8GB) /
  `MEM_SWAP` 클러스터를 함께 제시 — SQL에 안 잡히는 비-SQL Spark 잡까지 포착.

지수: "MV 리프레시가 매번 전체 재계산이었네. incremental로 바꾸면 spill이랑 시간 둘 다 줄겠다."

---

## Act 4 — 신뢰: "이 숫자들 믿어도 돼?"

> **"보고 올리기 전에, 이 집계 숫자들 정합성 확인해줘."**

- **로드되는 skill**: `monitoring-data-quality`.
- **뒤에서**: `queries/run_all_tests.sql`(T01–T17)를 실행 — 비용 원천이 sizing 테이블과
  reconcile되는지, serverless/classic 분할·UNION·duration 롤업·데이터 품질을 검사.
- **Genie 응답 (실측: 16/17 PASS)**:
  - ✅ 비용 분배(T01–T04): 귀속 classic $137,412 == sizing 합, diff 0.
  - ✅ serverless/classic 분할(T05–T06), ✅ UNION 정합(T07–T09: 3,585 = 851+2,734),
    ✅ 새니티(T13–T17: 음수/NULL/빈 테이블 없음).
  - ❌ **T11** (job-compute duration): 원천 929,405분 vs 롤업 845,008분 (~9%, 허용 5% 초과).
    "duration 롤업이 9% 어긋납니다 — 비용·사이징 판정에는 영향 없지만, 파이프라인의 duration
    집계를 점검하세요." → 스킬이 **실제 불일치를 잡아내** 신뢰 한계를 투명하게 보고.
- 지수는 비용·절감 수치는 안심하고 인용하고, duration 지표만 단서를 달아 보고.

---

## Act 5 — 마무리: 실행과 반복

지수는 하루 만에:
1. 저활용 Top 클러스터(harika/ketan/…)의 소유자에게 다운사이즈 요청 → 분기 **$10K+** 절감 확보.
2. MV_REFRESH 잡을 incremental로 전환 요청 → spill·런타임 동반 감소.
3. `Workload Monitoring Refresh` 잡이 **매일 06:00** 자동 갱신하므로, 다음 주에
   "지난주 대비 새로 오버사이징 된 클러스터"를 같은 skill로 다시 질의.

> **핵심**: 지수는 SQL을 한 줄도 직접 쓰지 않았다. 6개 skill이 검증된 쿼리 + 판정 taxonomy +
> 출력 계약을 캡슐화해, 워크스페이스 사용자·관리자가 **자연어로 컴퓨트를 최적화**하게 만든다.
> 이 skill들의 출력 계약(Problem/Action/$Savings/Effort/Risk/Docs)은 `fe-cost-optimization-report`의
> 근거 계층과 맞물려, FE가 만드는 고객용 비용 최적화 브리핑에 그대로 투입될 수 있다.

---

## 부록 — 질문 → skill 라우팅 빠른 표

| 관리자가 묻는 것 | 발동 skill |
|---|---|
| "세팅해줘 / 테이블 없어 / 갱신해줘" | `job-monitoring-setup` |
| "비용 얼마 / DBU / classic vs serverless / 제일 비싼 잡" | `compute-cost-analysis` |
| "오버사이징 / 줄이면 얼마 / 오토스케일 / 저활용" | `compute-right-sizing` |
| "왜 느려 / spill / shuffle / 디스크 풀 / EXPANDED_DISK" | `disk-spill-diagnosis` |
| "이 숫자 믿어도 돼 / 검증 / reconcile" | `monitoring-data-quality` |
| 광범위/모호 ("컴퓨트 상태 어때") | `job-cluster-monitoring`(라우터)가 분배 |
