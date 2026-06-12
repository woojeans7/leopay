package com.leopay.settlementworker.batch

import com.leopay.core.enums.MerchantStatus
import com.leopay.core.enums.SettlementStatus
import com.leopay.settlementworker.batch.processor.SettlementProcessor
import com.leopay.storage.entity.MerchantEntity
import com.leopay.storage.entity.SettlementDetailEntity
import com.leopay.storage.repository.MerchantRepository
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.LocalDate
import java.util.Optional

class SettlementProcessorTest {

    private val merchantRepository: MerchantRepository = mockk()
    private lateinit var processor: SettlementProcessor

    private val merchantId = 1L
    private val merchant = MerchantEntity(
        id = merchantId,
        businessNumber = "123-45-67890",
        name = "테스트 가맹점",
        bankCode = "004",
        accountNumber = "123-456-789",
        accountHolder = "테스트",
        feeRate = BigDecimal("0.0350"),
        status = MerchantStatus.ACTIVE,
    )

    @BeforeEach
    fun setUp() {
        processor = SettlementProcessor(merchantRepository, "2025-06-01")
    }

    @Test
    fun `정상 케이스 - amount에서 feeAmount를 차감한 settlementAmount를 반환한다`() {
        val detail = SettlementDetailEntity(
            id = 1L,
            settlementId = null,
            paymentId = 1L,
            merchantId = merchantId,
            settlementDate = LocalDate.now(),
            amount = BigDecimal("50000"),
            feeAmount = BigDecimal("1750.0000"),
        )

        every { merchantRepository.findById(merchantId) } returns Optional.of(merchant)

        val result = processor.process(detail)

        assertThat(result).isNotNull
        assertThat(result!!.settlementAmount).isEqualByComparingTo(BigDecimal("48250"))
        assertThat(result.amount).isEqualByComparingTo(BigDecimal("50000"))
        assertThat(result.feeAmount).isEqualByComparingTo(BigDecimal("1750.0000"))
        assertThat(result.feeRate).isEqualByComparingTo(BigDecimal("0.0350"))
    }

    @Test
    fun `원 단위 미만 절사 - 소수점 0점5 경계에서 버림 처리한다`() {
        // 10001 - 350.5000 = 9650.5 → FLOOR → 9650 (절사)
        val detail = SettlementDetailEntity(
            id = 2L,
            settlementId = null,
            paymentId = 2L,
            merchantId = merchantId,
            settlementDate = LocalDate.now(),
            amount = BigDecimal("10001"),
            feeAmount = BigDecimal("350.5000"),
        )

        every { merchantRepository.findById(merchantId) } returns Optional.of(merchant)

        val result = processor.process(detail)

        assertThat(result).isNotNull
        assertThat(result!!.settlementAmount).isEqualByComparingTo(BigDecimal("9650"))
    }

    @Test
    fun `원 단위 미만 절사 - 소수점 0점5 초과도 버림 처리한다`() {
        // 10001 - 350.4000 = 9650.6 → FLOOR → 9650 (절사, 올림 아님)
        val detail = SettlementDetailEntity(
            id = 3L,
            settlementId = null,
            paymentId = 3L,
            merchantId = merchantId,
            settlementDate = LocalDate.now(),
            amount = BigDecimal("10001"),
            feeAmount = BigDecimal("350.4000"),
        )

        every { merchantRepository.findById(merchantId) } returns Optional.of(merchant)

        val result = processor.process(detail)

        assertThat(result).isNotNull
        assertThat(result!!.settlementAmount).isEqualByComparingTo(BigDecimal("9650"))
    }

    @Test
    fun `가맹점 정보가 없으면 null을 반환한다`() {
        val detail = SettlementDetailEntity(
            id = 4L,
            settlementId = null,
            paymentId = 4L,
            merchantId = merchantId,
            settlementDate = LocalDate.now(),
            amount = BigDecimal("50000"),
            feeAmount = BigDecimal("1750.0000"),
        )

        every { merchantRepository.findById(merchantId) } returns Optional.empty()

        val result = processor.process(detail)

        assertThat(result).isNull()
    }
}