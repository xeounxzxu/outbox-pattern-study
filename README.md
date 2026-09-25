# Outbox Pattern

Spring Boot + Kotlin 기반 Gradle 멀티 모듈 프로젝트입니다.

## 기술 구성

- Java 25 LTS (빌드 및 실행 시 JDK 25 필요)
- Spring Boot 4.1.1
- Kotlin 2.3.21
- Gradle Wrapper / Kotlin DSL

## 모듈

| 모듈 | 역할 | 기본 포트 |
| --- | --- | --- |
| app/api | REST API 서버 | 8080 |
| app/worker | 스케줄링 기반 백그라운드 작업 서버 | 8081 |

두 모듈은 서로 의존하지 않으며 각각 실행 가능한 Boot JAR로 빌드됩니다.
루트 프로젝트는 공통 빌드 설정만 관리하며, app은 두 실행 모듈을 묶는 상위 프로젝트입니다.
Worker의 HTTP 서버는 Actuator 상태 확인용입니다.

## 실행

JDK 25를 설치하고 `JAVA_HOME`을 해당 JDK로 설정합니다. Gradle Toolchain은 두 모듈의 컴파일, 테스트, bootRun에 JDK 25를 사용합니다.

macOS에서는 다음과 같이 설정할 수 있습니다.

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 25)
export PATH="$JAVA_HOME/bin:$PATH"
```

각각 별도 터미널에서 실행합니다.

```sh
./gradlew :app:api:bootRun
./gradlew :app:worker:bootRun
```

```sh
curl http://localhost:8080/api/hello
curl http://localhost:8080/actuator/health
curl http://localhost:8081/actuator/health
```

## 빌드 및 테스트

```sh
./gradlew clean build
```

```sh
java -jar app/api/build/libs/api-0.0.1-SNAPSHOT.jar
java -jar app/worker/build/libs/worker-0.0.1-SNAPSHOT.jar
```

## 환경 변수

| 변수 | 기본값 | 설명 |
| --- | --- | --- |
| API_PORT | 8080 | API 서버 포트 |
| WORKER_PORT | 8081 | Worker 상태 확인 서버 포트 |
| WORKER_SCHEDULING_ENABLED | true | Worker 스케줄러 활성화 |
| WORKER_POLL_INTERVAL_MS | 5000 | 이전 작업 완료 후 대기 시간(ms) |

WorkerJob.poll()은 작업 구현을 추가할 확장 지점이며 현재 DEBUG 로그만 기록합니다.
DB, 메시지 브로커, 실제 Outbox 저장·발행·재시도 로직은 아직 포함하지 않습니다.
