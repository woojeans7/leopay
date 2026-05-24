# LeoPay — 간편결제 서비스

가맹점에 카드 결제 기능을 제공하는 간편결제 시스템입니다.
단순 구현이 아닌, **장애 시나리오를 의도적으로 재현하고 신뢰성 목표를 수치로 검증**하는 것을 목표로 합니다.

> 목표: 동시 100건 요청 시 중복 결제 0건

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| Language | Kotlin, Java 21 |
| Framework | Spring Boot 3.5.13 |
| API | Spring MVC, Spring WebFlux |
| ORM | Spring Data JPA, Hibernate |
| DB | MySQL, H2 (테스트) |
| Cache / Lock | Redis (분산락, 멱등성 키) |
| Messaging | Apache Kafka |
| Batch | Spring Batch |
| Monitoring | Micrometer, Prometheus, Grafana |
| Infra | Docker, Docker Compose, AWS EC2 |
| CI | GitHub Actions |

---

## 모듈 구조

```
payment-service/
├── core/
│   └── core-enum/           # 공유 Enum (PaymentStatus, PaymentMethod 등)
├── storage/
│   └── db-core/             # JPA Entity, Repository
├── support/
│   ├── logging/             # 공통 로깅 설정
│   └── monitoring/          # Micrometer + Prometheus
├── booking-api/             # 결제 요청/승인/취소 API (port: 8080)
├── mock-pg/                 # PG사 Mock 서버 — WebFlux (port: 8081)
├── notification-worker/     # 결제 알림 처리 — Kafka Consumer
└── settlement-worker/       # 일일 정산 배치 — Kafka Consumer + Spring Batch
```

---

## 핵심 도메인

### 가맹점 (Merchant)
- 가맹점 등록 (사업자번호, 상호명, 정산 계좌, 수수료율)
- 가맹점 상태 관리 (활성 / 비활성 / 정지)

### 결제 (Payment)
- 결제 요청 → Mock PG 승인 → 결제 완료
- 결제 상태 관리: `READY → IN_PROGRESS → APPROVED → CANCEL_IN_PROGRESS → CANCELED` / `IN_PROGRESS → FAILED` / `CANCEL_IN_PROGRESS → CANCEL_FAILED`
- 전체 취소 (부분 취소는 스코프 아웃)

### Mock PG
- WebFlux 기반 비동기 서버
- 지연 응답, 랜덤 실패, 타임아웃 시뮬레이션
- `PaymentGateway` 인터페이스 추상화 → 실제 PG 교체 가능한 구조

### 알림 (Notification)
- 결제 완료 이벤트 발행 (`payment.completed` Kafka 토픽)
- 실패 시 DLQ 이동 + 재처리

### 정산 (Settlement)
- Spring Batch 일일 정산 (매일 자정 기준)
- 가맹점별 결제 집계 → 수수료 차감 → 정산 금액 산출
- 정산 상태: `PENDING → COMPLETED → TRANSFERRED`
- BigDecimal 사용 (부동소수점 오차 방지)

---

## 동작 흐름

### 결제 생성

```
Client                  booking-api (8080)              mock-pg (8081)         DB (MySQL)
  │                            │                               │                    │
  │  POST /payments            │                               │                    │
  │──────────────────────────▶│                               │                    │
  │                            │ Redis 분산락 획득              │                    │
  │                            │ orderId 중복 체크              │                    │
  │                            │ merchant 조회 + 상태 검증      │                    │
  │                            │                               │                    │
  │                            │ payment 저장 (READY)          │                    │
  │                            │ history 기록                  │                    │──▶ INSERT payment
  │                            │ status → IN_PROGRESS          │                    │──▶ INSERT payment_history x2
  │                            │                               │                    │
  │                            │  POST /pg/approve             │                    │
  │                            │──────────────────────────────▶│                    │
  │                            │                               │ 90% 성공           │
  │                            │◀──────────────────────────────│ 10% 실패           │
  │                            │                               │                    │
  │                            │ status → APPROVED / FAILED    │                    │
  │                            │ history 기록                  │                    │──▶ UPDATE payment
  │                            │ outbox_event 저장             │                    │──▶ INSERT payment_history
  │                            │ Redis 락 해제                 │                    │──▶ INSERT outbox_event
  │                            │                               │                    │
  │◀──────────────────────────│                               │                    │
  │  PaymentCreateResponse     │                               │                    │
```

### 결제 취소

```
Client                  booking-api (8080)              mock-pg (8081)         DB (MySQL)
  │                            │                               │                    │
  │  POST /payments/{id}/cancel│                               │                    │
  │──────────────────────────▶│                               │                    │
  │                            │ Redis 분산락 획득              │                    │
  │                            │ payment 조회                  │                    │
  │                            │ 상태 전환 검증 (APPROVED만 가능)│                    │
  │                            │                               │                    │
  │                            │ status → CANCEL_IN_PROGRESS   │                    │──▶ UPDATE payment
  │                            │ history 기록                  │                    │──▶ INSERT payment_history
  │                            │                               │                    │
  │                            │  POST /pg/cancel              │                    │
  │                            │──────────────────────────────▶│                    │
  │                            │                               │ 95% 성공           │
  │                            │◀──────────────────────────────│ 5% 실패            │
  │                            │                               │                    │
  │                            │ status → CANCELED / CANCEL_FAILED                  │
  │                            │ history 기록                  │                    │──▶ UPDATE payment
  │                            │ outbox_event 저장             │                    │──▶ INSERT payment_history
  │                            │ Redis 락 해제                 │                    │──▶ INSERT outbox_event
  │                            │                               │                    │
  │◀──────────────────────────│                               │                    │
  │  PaymentResponse           │                               │                    │
```

### Kafka 이벤트 발행 (Transactional Outbox Pattern)

결제 트랜잭션 내에서 outbox_events에 이벤트를 저장하고, 별도 폴러가 Kafka로 발행한다.
Kafka 장애 시에도 이벤트가 DB에 보존되어 복구 후 자동 재발행된다.

```
DB (outbox_events)
  │
  │  OutboxPublisher — @Scheduled(fixedDelay=5s)
  │  published=false 이벤트 조회 → Kafka 발행 → published=true (REQUIRES_NEW 트랜잭션)
  │  발행 실패 시 DB 변경 없음 → 다음 폴링에서 자동 재시도
  ▼
Kafka (payment.events 토픽)
  │
  ├──▶ notification-worker  → notification 테이블 저장 (status=SENT)
  └──▶ settlement-worker    → 정산 처리 (Week 4 배치 연동)
```

### Kafka Consumer 장애 처리 (DLQ)

Consumer 처리 실패 시 메시지를 유실하지 않고 DLT 토픽에 보관한다.
실패 이력을 DB에 남겨 누락 건 추적 및 수동 재처리가 가능하다.

```
Kafka (payment.events 토픽)
  │
  └──▶ notification-worker Consumer
          │ 처리 성공 → notification 저장 (status=SENT)
          │ 처리 실패 → DefaultErrorHandler: 1초 간격 3회 재시도
                          │ 재시도 성공 → notification 저장 (status=SENT)
                          │ 최종 실패  → DeadLetterPublishingRecoverer
                                          │
                                          ▼
                                     payment.events.DLT 토픽
                                          │
                                          └──▶ DltEventConsumer
                                                  → notification 저장 (status=FAILED)
                                                  → 운영자 확인 후 수동 재처리
```

---

## 성능 목표

| 구분 | TPS |
|------|-----|
| 평시 | 300 |
| 피크 | 1,000 |

> M3 (성능코어 8개) + Docker Compose 로컬 환경 (RAM 18GB) 기준.
> 이 수치에서 분산락 경합, 커넥션풀 고갈, Kafka 백프레셔 등 실제 문제가 재현됨.

---

## 신뢰성 설계

### Type A: 재현 후 해결 (의도적 장애 유발 → 수치 확인 → 수정 → 재검증)

> 면접 포인트: "어떻게 발견했어요?"에 테스트 코드 + 수치로 답할 수 있어야 함

| # | 문제 | 재현 방법 | 해결 | 상태 |
|---|------|----------|------|------|
| A1 | 분산락 없을 때 중복 결제 발생 | `test/concurrency-without-lock` 브랜치에서 100 동시 요청 → failCount > 0 확인 | Redis 분산락 적용 | ✅ 완료 |
| A2 | `withLock` 재시도 없음 → 99개 요청 에러 | 100 동시 요청 → 성공 1건, 실패 99건 (waitTime 초과) 확인 | Redisson으로 교체 (pub/sub 기반 대기) | ✅ 완료 |
| A3 | 멱등성 미처리 → 동일 orderId 중복 결제 | 락 TTL 만료 후 동일 orderId 재요청 → 중복 저장 확인 | `order_id` UNIQUE + `findByOrderId` 중복 체크 | ✅ 완료 |
| A4 | Kafka 컨슈머 처리 실패 → 메시지 유실 | Consumer 예외 발생 시 메시지 버려짐 확인 | DLQ + 재처리 | ⏳ Week 3 |
| A5 | 대량 데이터 배치 성능 저하 | 대량 정산 데이터 처리 시간 측정 | Chunk 사이즈 튜닝 | ⏳ Week 4 |

---

### Type B: 예방 설계 (처음부터 설계에 포함되어야 하는 원칙)

> 의도적으로 빼고 터뜨리는 게 자연스럽지 않은 것들. "없으면 안 되는 것"을 처음부터 올바르게 구현.

| # | 항목 | 설계 근거 | 상태 |
|---|------|----------|------|
| B1 | 상태 전이 제약 (`canTransitionTo`) | 불법 전이를 코드 레벨에서 차단 | ✅ 완료 |
| B2 | PG 응답 지연 → WebClient 타임아웃 + fallback | 타임아웃 없으면 스레드 고갈 위험. 부하 수준별 동적 설정 (normal: 10s / peak: 20s) | ✅ 완료 |
| B3 | DB 커밋 성공 + Kafka 발행 실패 → Outbox 패턴 | DB-Kafka 원자성 보장. 장애 시 이벤트 보존 + 자동 재발행 | ✅ 완료 |
| B4 | 컨슈머 중복 소비 → 컨슈머 멱등성 처리 | 동일 이벤트 재처리 시 중복 알림/정산 방지 | ⏳ Week 3 |
| B5 | `PaymentGateway` 인터페이스 추상화 | PG사 교체 가능성 대비. 확장성 원칙 | ✅ 완료 |
| B6 | 배치 중간 실패 → Skip/Retry + 재시작 포인트 | 부분 실패 시 전체 재처리 방지 | ⏳ Week 4 |
| B7 | 수수료 부동소수점 오차 → BigDecimal | 금액 계산 오차는 처음부터 막아야 함 | ⏳ Week 4 |

---

## 스코프 아웃

- 실제 PG사 연동 (TossPayments 등)
- 부분 취소
- 실제 알림 발송 (SMS, 이메일, 푸시)
- 사용자 인증/인가 (X-User-Id 헤더로 대체 — Auth는 API Gateway 책임)
- 프론트엔드

---

## 실행 방법

```bash
# 인프라 실행 (MySQL, Redis, Kafka, Prometheus, Grafana)
docker-compose up -d

# 모듈별 실행
./gradlew :booking-api:bootRun
./gradlew :mock-pg:bootRun
./gradlew :notification-worker:bootRun
./gradlew :settlement-worker:bootRun
```
