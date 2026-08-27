package dev.vality.ccreporter.ingestion.withdrawal;

import dev.vality.fistful.base.Cash;
import dev.vality.fistful.base.CurrencyRef;
import dev.vality.fistful.withdrawal.BodyChange;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WithdrawalEventProjectorTest {

    private final WithdrawalEventProjector projector = new WithdrawalEventProjector();

    @Test
    void bodyChangedUsesNewBodyAmountAndCurrency() {
        var payload = new TimestampedChange()
                .setOccuredAt("2026-08-20T11:20:00Z")
                .setChange(Change.body_changed(
                        new BodyChange()
                                .setOldBody(new Cash()
                                        .setAmount(1000L)
                                        .setCurrency(new CurrencyRef("RUB")))
                                .setNewBody(new Cash()
                                        .setAmount(2000L)
                                        .setCurrency(new CurrencyRef("USD")))
                ));
        var event = new MachineEvent()
                .setEventId(2L)
                .setSourceId("withdrawal-1")
                .setCreatedAt("2026-08-20T11:20:01Z");

        var updates = projector.project(event, payload);

        assertThat(updates).hasSize(1);
        assertThat(updates.getFirst().getAmount()).isEqualTo(2000L);
        assertThat(updates.getFirst().getCurrency()).isEqualTo("USD");
    }
}
