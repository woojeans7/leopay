INSERT INTO merchant (business_number, name, bank_code, account_number, account_holder, fee_rate, status)
VALUES
    ('123-45-67890', '테스트 가맹점 A', 'KB', '12345678901234', '홍길동', 0.0350, 'ACTIVE'),
    ('234-56-78901', '테스트 가맹점 B', 'SHINHAN', '98765432109876', '김철수', 0.0200, 'ACTIVE'),
    ('345-67-89012', '비활성 가맹점', 'WOORI', '11122233344455', '이영희', 0.0300, 'INACTIVE');
