# MySQL Transactional Outbox 로컬 동작 검증

## 개요

`/Users/jeoung-gyu/Workspace/outbox-pattern` 프로젝트에서 MySQL 지원 로컬 라이브러리를 적용한 뒤, 실제 Compose MySQL·Kafka와 API·Worker 실행 JAR로 동작을 검증했다.

- 실행일: 2026-09-26, Asia/Seoul
- 최종 로컬 실행 ID: `20260926T024802Z` (11:48:02 KST 시작)
- 결과: **로컬 TC 8개 PASS + 자동 통합 테스트 4개 PASS**
- 빌드: `./gradlew build --rerun-tasks --console=plain` → `BUILD SUCCESSFUL in 38s`, 17개 작업 재실행
- 검증 방식: 실제 HTTP 요청, MySQL SQL 조회, API 프로세스 강제 종료, Kafka 중단·복구, 실제 Kafka 소비
- 실행 스크립트: [verify-local-mysql.py](../scripts/verify-local-mysql.py)
- 원본 결과: [results.json](local-mysql-evidence-20260926T024802Z/results.json)

## 내용

### 실행 환경과 범위

| 항목 | 실제 사용 값 |
| --- | --- |
| OS / Java | macOS / Oracle GraalVM Java 25 LTS |
| Spring Boot / Kotlin | 4.1.1 / 2.3.21 |
| Outbox 라이브러리 | Maven Local의 `outbox-kafka-spring:4.0.1-SNAPSHOT` |
| 공통 라이브러리 | `commons:3.0.1-SNAPSHOT` |
| MySQL | Compose `mysql:8.4`, 서버 조회 결과 `8.4.11` |
| Kafka 브로커 | Compose `apache/kafka:4.1.1` |
| API / Worker | 실제 실행 JAR, localhost:8080 / localhost:8081 |
| DB | localhost:3306, `outbox`, InnoDB |
| 시간대 | JVM UTC, MySQL 세션 `+00:00` |
| Flyway | `db/mysql`의 V1 Outbox, V2 Orders 모두 success=1 |
| 테스트 토픽 | `outbox.tc.20260926t024802z`, 파티션 1, 복제 수 1 |
| 기본 토픽 | `orders.created`도 초기화했으나 TC 메시지는 실행 전용 토픽으로 격리 |
| 처리/락/전송 타임아웃 | 프로젝트 기본값 200ms / 5s / 10000ms |

로컬 TC에서는 Testcontainers 대신 프로젝트의 `compose.yml`을 사용했다. API와 Worker는 `java -Duser.timezone=UTC -jar ...`로 실행했다. 토픽과 락 소유자 ID만 실행별로 지정했다. 기존 자동 통합 테스트 4개는 별도로 Testcontainers에서 실행했다.

### 로컬 테스트 케이스

| ID | 절차 | 기대 결과 | 실제 결과 | 판정 |
| --- | --- | --- | --- | --- |
| TC-L01 | MySQL·Kafka 시작 → API 시작 → API 락 확인 → Worker 시작 → health/Flyway 조회 | 두 앱 UP, 마이그레이션 성공, API 락 유지 | 두 health HTTP 200 및 status=UP, V1/V2 success=1, API 소유 확인 | PASS |
| TC-L02 | 정상 주문 POST → 업무 테이블과 Outbox JOIN 조회 → processed 대기 | HTTP 201, 동일 주문 ID로 저장, 발행 완료 | 주문/Outbox 1번 저장 및 PROCESSED 확인 | PASS |
| TC-L03 | 빈 상품명·수량 0으로 POST, 전후 테이블 건수 비교 | HTTP 400, 저장 건수 변화 없음 | HTTP 400, orders/outbox 모두 1 → 1 | PASS |
| TC-L04 | Kafka stop → 새 주문 POST → 12초 관찰 | HTTP 201, 전송 실패에도 업무 데이터와 미처리 이벤트 보존 | 주문 2번 저장, 11회 SQL 표본 모두 PENDING; 기본 전송 제한 10초보다 긴 구간 관찰 | PASS |
| TC-L05 | Kafka 중단 상태에서 API에 SIGKILL → 락 소유자 조회 | 락 만료 후 Worker 인계, 이벤트 보존 | 약 4.81초 후 Worker 소유 확인, 주문 2번 PENDING 유지 | PASS |
| TC-L06 | Kafka start → 미처리 이벤트 조회 | Worker가 재시도하여 발행 완료 | 주문 2번 PROCESSED, Worker 소유 확인 | PASS |
| TC-L07 | API 재시작 → 정상 주문 POST → 락/processed 확인 | Worker 락 유지, 재시작한 API의 주문도 처리 | 주문 3번 HTTP 201 및 PROCESSED, Worker 소유 유지 | PASS |
| TC-L08 | 실제 Kafka console consumer로 실행 토픽 처음부터 소비 | 세 주문 ID와 source/sequence/type 헤더 수신 | 세 주문 모두 수신, 각 메시지에서 해당 헤더 확인 | PASS |

TC-L02/L04/L07 공통 요청 형태:

```http
POST /api/orders
Content-Type: application/json

{"productName":"<실행 ID>-<시나리오>","quantity":2}
```

TC-L03 입력:

```json
{"productName":"","quantity":0}
```

### 실제 저장·발행 증거

| 시나리오 | 주문 ID / Kafka key | Outbox ID | 최종 상태 |
| --- | --- | --- | --- |
| 정상 주문 | `73c2a572-13d6-47af-be65-70e96fec1f19` | 1 | PROCESSED |
| Kafka 장애 중 저장 후 복구 | `2749ad15-9315-47df-9e24-7afd60fd5d6a` | 2 | PROCESSED |
| API 재시작 후 주문 | `4056b97a-467e-472c-a8db-5fe1a3aa9a6b` | 3 | PROCESSED |

최종 SQL 조회 결과(UTF-8 클라이언트):

```text
73c2a572-13d6-47af-be65-70e96fec1f19  20260926T024802Z-정상-주문    2  1  1
2749ad15-9315-47df-9e24-7afd60fd5d6a  20260926T024802Z-장애-복구    2  2  1
4056b97a-467e-472c-a8db-5fe1a3aa9a6b  20260926T024802Z-재시작-주문  2  3  1
```

열 순서: 주문 ID, 상품명, 수량, Outbox ID, `processed IS NOT NULL`.

- 최종 미처리 이벤트: **0건**
- 테스트가 시작한 앱 종료 후 락 레코드: **0건**
- 락 인계: `tc-api-20260926T024802Z` → `tc-worker-20260926T024802Z`
- API 재시작 시에도 Worker가 락을 유지했다.

Kafka에서 수신한 장애 복구 메시지 본문:

```json
{"orderId":"2749ad15-9315-47df-9e24-7afd60fd5d6a","productName":"20260926T024802Z-장애-복구","quantity":2}
```

세 메시지에서 확인한 헤더:

- `event-type: OrderCreated`
- `content-type: application/json`
- `x-source: outbox-pattern`
- `x-sequence`: 각각 Outbox ID 1, 2, 3을 나타내는 8바이트 값

[Kafka 원본 출력](local-mysql-evidence-20260926T024802Z/kafka-messages.txt)에는 바이너리 sequence 헤더의 제어 문자가 포함되어 있다. 가독성 있는 이스케이프 표현은 [JSON 결과](local-mysql-evidence-20260926T024802Z/results.json)의 TC-L08에서 확인할 수 있다.

### 자동 통합 테스트 재실행

캐시 결과를 재사용하지 않도록 `--rerun-tasks`를 지정했다.

| ID | 실제 테스트 | 결과 |
| --- | --- | --- |
| TC-A01 | API commits order and pending event without Kafka | PASS |
| TC-A02 | invalid request cannot create order or event | PASS |
| TC-A03 | rollback cancels business data and outbox together | PASS |
| TC-A04 | API publishes and worker takes over pending events after Kafka outage | PASS |

- API: tests=3, failures=0, errors=0, skipped=0
- Worker: tests=1, failures=0, errors=0, skipped=0
- 업무 저장과 Outbox 저장의 원자적 롤백은 TC-A03으로 검증했다. HTTP API에 테스트용 실패 엔드포인트를 추가하지 않았다.
- [API 테스트 XML](../app/api/build/test-results/test/TEST-com.example.outbox.api.ApiApplicationTests.xml)
- [Worker 테스트 XML](../app/worker/build/test-results/test/TEST-com.example.outbox.worker.WorkerApplicationTests.xml)

### 실행 중 발견한 검증 도구 문제

1. 첫 실행 `20260926T024636Z`에서는 API health 전체 JSON을 `{"status":"UP"}`와 정확히 비교했다. 실제 Boot health 응답에는 `groups: ["liveness", "readiness"]`도 있어서 앱이 정상 UP인데도 TC가 타임아웃됐다. `status` 필드를 기준으로 판정하도록 수정한 뒤 로컬 TC 전체를 다시 실행했고 8개 모두 통과했다. 첫 실행 실패 기록도 [보존했다](local-mysql-evidence-20260926T024636Z/results.json).
2. MySQL CLI 기본 문자셋 때문에 첫 결과 JSON의 SQL 출력에서 한글이 `?`로 표시됐다. Kafka 본문에는 원문이 보존됐고, `--default-character-set=utf8mb4`로 최종 DB를 다시 조회하여 한글 저장을 확인했다. 재실행 스크립트에도 이 옵션을 반영했다. 이 출력 옵션 변경 후 전체 장애 시나리오를 추가 재실행하지는 않았으며, UTF-8 DB 조회를 직접 실행해 확인했다.

애플리케이션 코드 변경 없이 검증 스크립트의 판정·출력 설정을 수정했다.

### 재현 절차

다음은 프로젝트 전용 개발 환경에서 실행한다. 스크립트는 Kafka 서비스를 실제로 중단·복구하므로 해당 Kafka를 다른 작업에서 사용하지 않는 상태여야 한다. API/Worker 포트가 이미 사용 중이면 스크립트가 실패하며 기존 프로세스를 종료하지 않는다.

```sh
cd /Users/jeoung-gyu/Workspace/outbox-pattern

# 라이브러리를 변경했거나 다른 머신이라면 먼저 README의 Maven Local 배포 절차 실행
./gradlew build --rerun-tasks --console=plain

docker compose up -d --wait mysql kafka
docker compose run --rm kafka-init

python3 scripts/verify-local-mysql.py
```

스크립트는 실행별 `docs/local-mysql-evidence-<UTC 실행 ID>/`에 JSON 결과, 앱 로그, Kafka 메시지와 컨테이너 상태를 저장한다. 예외가 발생하면 FAIL 기록을 남기고 자신이 시작한 앱 프로세스를 종료한다. 테스트 중 중단한 Kafka는 복구한다.

최종 DB 직접 조회:

```sh
docker compose exec -T -e MYSQL_PWD=outbox mysql mysql \
  --default-character-set=utf8mb4 -uoutbox -D outbox -e \
  'SELECT b.id,b.product_name,o.id,o.processed FROM orders b JOIN outbox_kafka o ON o.`key`=b.id ORDER BY o.id;'
```

### 실행 로그

- [API](local-mysql-evidence-20260926T024802Z/tc-api-20260926T024802Z.log)
- [Worker](local-mysql-evidence-20260926T024802Z/tc-worker-20260926T024802Z.log)
- [재시작 API](local-mysql-evidence-20260926T024802Z/tc-api-20260926T024802Z-restart.log)
- [검증 시점 Compose 상태](local-mysql-evidence-20260926T024802Z/compose-status.txt)

## 메모

- 검증이 시작한 API·Worker 프로세스는 종료했다. MySQL·Kafka는 healthy 상태로 유지하여 사용자가 데이터를 조회할 수 있다.
- 주문 3건, Outbox 3건, 실행 전용 토픽 및 MySQL 볼륨은 보존했다. 기존 데이터를 삭제하지 않았다.
- 개발 컨테이너가 더 이상 필요 없으면 `docker compose stop mysql kafka`로 중지할 수 있다. 볼륨 삭제는 필요하지 않다.
- 이번 결과는 로컬 단일 MySQL·단일 Kafka 브로커와 두 앱에서의 기능 검증이다. 부하·장시간 안정성·다중 브로커 장애·MySQL 재시작 복구는 이번 로컬 TC 범위에 포함하지 않는다.
- Kafka 소비 검증은 세 기대 이벤트의 수신을 확인했다. 모든 상황에서의 중복 부재나 exactly-once를 보장하는 검증은 아니다. 라이브러리의 at-least-once 특성상 소비자 멱등 처리는 필요하다.
- 커밋은 수행하지 않았다.
