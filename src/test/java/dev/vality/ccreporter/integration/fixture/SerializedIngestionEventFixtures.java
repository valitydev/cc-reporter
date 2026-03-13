package dev.vality.ccreporter.integration.fixture;

import dev.vality.machinegun.eventsink.MachineEvent;

import java.util.List;

/**
 * Сохраняет старую точку входа для ingestion fixtures, но сами события уже разложены по отдельным классам.
 */
public final class SerializedIngestionEventFixtures {

    public static final String PAYMENT_INVOICE_ID = PaymentIngestionEventFixtures.PAYMENT_INVOICE_ID;
    public static final String PAYMENT_ID = PaymentIngestionEventFixtures.PAYMENT_ID;
    public static final String REAL_PAYMENT_INVOICE_ID = RealPaymentIngestionEventFixtures.PAYMENT_INVOICE_ID;
    public static final String REAL_PAYMENT_ID = RealPaymentIngestionEventFixtures.PAYMENT_ID;
    public static final String LEGACY_PAYMENT_INVOICE_ID = RealPaymentIngestionEventFixtures.LEGACY_PAYMENT_INVOICE_ID;
    public static final String LEGACY_PAYMENT_ID = RealPaymentIngestionEventFixtures.LEGACY_PAYMENT_ID;
    public static final String WITHDRAWAL_ID = WithdrawalIngestionEventFixtures.WITHDRAWAL_ID;
    public static final String WITHDRAWAL_SESSION_ID = WithdrawalIngestionEventFixtures.WITHDRAWAL_SESSION_ID;
    public static final String REAL_WITHDRAWAL_ID = RealWithdrawalIngestionEventFixtures.WITHDRAWAL_ID;

    private SerializedIngestionEventFixtures() {
    }

    public static List<MachineEvent> paymentEvents() {
        return PaymentIngestionEventFixtures.paymentEvents();
    }

    public static List<MachineEvent> paymentProxyStateFallbackEvents() {
        return PaymentIngestionEventFixtures.paymentProxyStateFallbackEvents();
    }

    public static List<MachineEvent> failedPaymentEvents() {
        return PaymentIngestionEventFixtures.failedPaymentEvents();
    }

    public static List<MachineEvent> realPaymentEvents() {
        return RealPaymentIngestionEventFixtures.paymentEvents();
    }

    public static List<MachineEvent> legacyPaymentEvents() {
        return RealPaymentIngestionEventFixtures.legacyPaymentEvents();
    }

    public static List<MachineEvent> paymentCollectionEvents() {
        return RealPaymentIngestionEventFixtures.paymentCollectionEvents();
    }

    public static List<MachineEvent> withdrawalEvents() {
        return WithdrawalIngestionEventFixtures.withdrawalEvents();
    }

    public static List<MachineEvent> realWithdrawalEvents() {
        return RealWithdrawalIngestionEventFixtures.withdrawalEvents();
    }

    public static List<MachineEvent> withdrawalCollectionEvents() {
        return RealWithdrawalIngestionEventFixtures.withdrawalCollectionEvents();
    }

    public static List<MachineEvent> withdrawalSessionEvents() {
        return WithdrawalIngestionEventFixtures.withdrawalSessionEvents();
    }
}
