-- ============================================================
-- LeoPay Schema
-- ============================================================

-- ── MERCHANT (가맹점) ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS merchant (
    id             BIGINT       NOT NULL AUTO_INCREMENT,
    business_number VARCHAR(20)  NOT NULL COMMENT '사업자등록번호',
    name           VARCHAR(100) NOT NULL COMMENT '상호명',
    bank_code      VARCHAR(50)  NOT NULL COMMENT '정산 은행 코드',
    account_number VARCHAR(30)  NOT NULL COMMENT '정산 계좌번호',
    account_holder VARCHAR(100) NOT NULL COMMENT '예금주',
    fee_rate       DECIMAL(5,4) NOT NULL COMMENT '수수료율 (예: 0.0350 = 3.5%)',
    status         VARCHAR(20)  NOT NULL COMMENT 'ACTIVE / INACTIVE / SUSPENDED',
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_merchant_business_number (business_number)
) DEFAULT CHARACTER SET utf8mb4;

-- ── PAYMENT (결제) ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS payment (
    id                 BIGINT       NOT NULL AUTO_INCREMENT,
    payment_key        VARCHAR(64)  NOT NULL COMMENT '결제 고유 키 (멱등성 키)',
    merchant_id        BIGINT       NOT NULL COMMENT '가맹점 ID',
    user_id            VARCHAR(64)  NOT NULL COMMENT 'X-User-Id 헤더 값',
    amount             DECIMAL(12,0) NOT NULL COMMENT '결제 금액 (원 단위)',
    method             VARCHAR(20)  NOT NULL COMMENT 'CARD 등',
    status             VARCHAR(20)  NOT NULL COMMENT 'READY / IN_PROGRESS / APPROVED / CANCEL_IN_PROGRESS / CANCELED / CANCEL_FAILED / FAILED',
    pg_transaction_id  VARCHAR(64)  NULL     COMMENT 'PG사 거래 고유번호',
    cancel_reason      VARCHAR(200) NULL     COMMENT '취소 사유',
    approved_at        DATETIME     NULL     COMMENT 'PG 승인 시각',
    canceled_at        DATETIME     NULL     COMMENT '취소 완료 시각',
    created_at         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_payment_payment_key (payment_key),
    CONSTRAINT fk_payment_merchant FOREIGN KEY (merchant_id) REFERENCES merchant (id),
    INDEX idx_payment_merchant_status  (merchant_id, status),
    INDEX idx_payment_merchant_approved (merchant_id, approved_at),
    INDEX idx_payment_user             (user_id, created_at),
    INDEX idx_payment_created          (created_at)
) DEFAULT CHARACTER SET utf8mb4;

-- ── PAYMENT_HISTORY (결제 이력) ────────────────────────────
CREATE TABLE IF NOT EXISTS payment_history (
    id              BIGINT       NOT NULL AUTO_INCREMENT,
    payment_id      BIGINT       NOT NULL COMMENT '결제 ID',
    previous_status VARCHAR(20)  NULL     COMMENT '변경 전 상태',
    new_status      VARCHAR(20)  NOT NULL COMMENT '변경 후 상태',
    reason          VARCHAR(500) NULL     COMMENT '상태 변경 사유',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_history_payment FOREIGN KEY (payment_id) REFERENCES payment (id),
    INDEX idx_history_payment (payment_id, created_at)
) DEFAULT CHARACTER SET utf8mb4;

-- ── NOTIFICATION (알림) ────────────────────────────────────
CREATE TABLE IF NOT EXISTS notification (
    id             BIGINT       NOT NULL AUTO_INCREMENT,
    payment_id     BIGINT       NOT NULL COMMENT '결제 ID',
    type           VARCHAR(20)  NOT NULL COMMENT 'PAYMENT_COMPLETED / PAYMENT_CANCELED',
    status         VARCHAR(20)  NOT NULL COMMENT 'PENDING / SENT / FAILED',
    retry_count    INT          NOT NULL DEFAULT 0 COMMENT '재시도 횟수',
    failure_reason VARCHAR(500) NULL     COMMENT '실패 사유',
    sent_at        DATETIME     NULL     COMMENT '발송 완료 시각',
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_notification_payment FOREIGN KEY (payment_id) REFERENCES payment (id),
    INDEX idx_notification_status  (status, created_at),
    INDEX idx_notification_payment (payment_id)
) DEFAULT CHARACTER SET utf8mb4;

-- ── SETTLEMENT (정산) ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS settlement (
    id                BIGINT       NOT NULL AUTO_INCREMENT,
    merchant_id       BIGINT       NOT NULL COMMENT '가맹점 ID',
    settlement_date   DATE         NOT NULL COMMENT '정산 기준일',
    total_count       INT          NOT NULL DEFAULT 0 COMMENT '총 결제 건수',
    total_amount      DECIMAL(15,0) NOT NULL DEFAULT 0 COMMENT '총 결제 금액',
    fee_amount        DECIMAL(15,4) NOT NULL DEFAULT 0 COMMENT '수수료 금액',
    settlement_amount DECIMAL(15,0) NOT NULL DEFAULT 0 COMMENT '정산 금액 (total - fee)',
    applied_fee_rate  DECIMAL(5,4) NOT NULL COMMENT '적용 수수료율 스냅샷',
    status            VARCHAR(20)  NOT NULL COMMENT 'PENDING / COMPLETED / TRANSFERRED',
    completed_at      DATETIME     NULL     COMMENT '정산 완료 시각',
    transferred_at    DATETIME     NULL     COMMENT '송금 완료 시각',
    created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_settlement_merchant_date (merchant_id, settlement_date),
    CONSTRAINT fk_settlement_merchant FOREIGN KEY (merchant_id) REFERENCES merchant (id),
    INDEX idx_settlement_status (status),
    INDEX idx_settlement_date   (settlement_date)
) DEFAULT CHARACTER SET utf8mb4;

-- ── SETTLEMENT_DETAIL (정산 상세) ──────────────────────────
CREATE TABLE IF NOT EXISTS settlement_detail (
    id            BIGINT        NOT NULL AUTO_INCREMENT,
    settlement_id BIGINT        NOT NULL COMMENT '정산 ID',
    payment_id    BIGINT        NOT NULL COMMENT '결제 ID',
    amount        DECIMAL(12,0) NOT NULL COMMENT '결제 금액 스냅샷',
    fee_amount    DECIMAL(15,4) NOT NULL COMMENT '수수료 스냅샷',
    created_at    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_detail_settlement FOREIGN KEY (settlement_id) REFERENCES settlement (id),
    CONSTRAINT fk_detail_payment    FOREIGN KEY (payment_id)    REFERENCES payment (id),
    INDEX idx_detail_settlement (settlement_id),
    INDEX idx_detail_payment    (payment_id)
) DEFAULT CHARACTER SET utf8mb4;

-- ── OUTBOX_EVENT (아웃박스 이벤트) ────────────────────────
CREATE TABLE IF NOT EXISTS outbox_event (
    id             BIGINT       NOT NULL AUTO_INCREMENT,
    aggregate_type VARCHAR(50)  NOT NULL COMMENT '이벤트 주체 타입 (PAYMENT 등)',
    aggregate_id   BIGINT       NOT NULL COMMENT '이벤트 주체 PK',
    event_type     VARCHAR(50)  NOT NULL COMMENT 'PAYMENT_APPROVED 등',
    payload        TEXT         NOT NULL COMMENT 'JSON 페이로드',
    published      BOOLEAN      NOT NULL DEFAULT FALSE COMMENT '발행 완료 여부',
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at   DATETIME     NULL     COMMENT 'Kafka 발행 완료 시각',
    PRIMARY KEY (id),
    INDEX idx_outbox_unpublished (published, created_at)
) DEFAULT CHARACTER SET utf8mb4;
