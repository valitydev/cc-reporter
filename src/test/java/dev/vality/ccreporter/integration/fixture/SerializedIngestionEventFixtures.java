package dev.vality.ccreporter.integration.fixture;

import dev.vality.machinegun.eventsink.MachineEvent;

import java.util.List;

/**
 * Сохраняет старую точку входа для ingestion fixtures, но сами события уже разложены по отдельным классам.
 */
public final class SerializedIngestionEventFixtures {

    public static final String PAYMENT_INVOICE_ID = PaymentIngestionEventFixtures.PAYMENT_INVOICE_ID;
    public static final String PAYMENT_ID = PaymentIngestionEventFixtures.PAYMENT_ID;
    public static final String WITHDRAWAL_ID = WithdrawalIngestionEventFixtures.WITHDRAWAL_ID;
    public static final String WITHDRAWAL_SESSION_ID = WithdrawalIngestionEventFixtures.WITHDRAWAL_SESSION_ID;

    private SerializedIngestionEventFixtures() {
    }

    public static List<MachineEvent> paymentEvents() {
        return PaymentIngestionEventFixtures.paymentEvents();
    }

    public static List<MachineEvent> withdrawalEvents() {
        return WithdrawalIngestionEventFixtures.withdrawalEvents();
    }

    public static List<MachineEvent> withdrawalSessionEvents() {
        return WithdrawalIngestionEventFixtures.withdrawalSessionEvents();
    }
}
