# Job Cluster Monitoring

Job & Cluster 워크로드 프로파일링 파이프라인과 Right-Sizing 대시보드를 Databricks Asset Bundles(DAB)로 배포합니다.

## Preview
![alt text](https://github.com/hurcy/job-cluster-monitoring/blob/main/dashboard_example.png)

## 프로젝트 구조

```
job-cluster-monitoring/
├── databricks.yml                  # DAB 번들 설정 (변수, 타겟 정의)
├── resources/
│   ├── pipelines.yml               # Lakeflow SDP 파이프라인 리소스
│   └── dashboard.yml               # AI/BI 대시보드 리소스
├── workload_analysis/
│   ├── transformations/            # 파이프라인 SQL (MV/ST 정의)
│   ├── explorations/               # 탐색용 노트북
│   └── validation/                 # 데이터 품질 검증 쿼리
└── dashboard/
    └── *.lvdash.json               # 대시보드 정의 파일
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
| `catalog` | Unity Catalog 카탈로그 이름 | `hurcy` |
| `system_copies_schema` | 시스템 테이블 MV 사본 스키마 | `default` |
| `analytics_schema` | 분석 MV 출력 스키마 | `test` |
| `workspace_id` | 분석 대상 워크스페이스 ID | `984752964297111` |
| `start_date` | 분석 시작일 | `2025-01-01` |
| `end_date` | 분석 종료일 | `2026-03-22` |
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

> **주의: 카탈로그 변경 시** — 기존 파이프라인의 카탈로그를 변경하면 배포가 실패합니다.
> 이 경우 기존 리소스를 먼저 삭제한 후 재배포해야 합니다:
> ```bash
> databricks bundle destroy -t <target> --auto-approve
> databricks bundle deploy -t <target>
> ```

### 5. 파이프라인 실행

```bash
# 파이프라인 시작 (증분 업데이트)
databricks bundle run instance_workload_profiles -t prod

# 전체 새로고침 (full refresh)
databricks bundle run instance_workload_profiles -t prod --full-refresh-all
```

### 6. 대시보드

대시보드는 Global Filters를 통해 **카탈로그**, **스키마**, **조회 기간**을 런타임에 변경할 수 있습니다.
파이프라인 출력 카탈로그/스키마가 변경되면, 대시보드 JSON의 `p_catalog`/`p_schema` 파라미터 기본값도 함께 변경해야 합니다.

### 7. 배포 상태 확인 / 삭제

```bash
# 배포 상태 확인
databricks bundle validate -t prod

# 리소스 삭제
databricks bundle destroy -t prod
```

## License
MIT License
