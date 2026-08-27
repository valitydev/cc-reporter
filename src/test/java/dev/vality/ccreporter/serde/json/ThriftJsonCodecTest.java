package dev.vality.ccreporter.serde.json;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.config.JacksonConfig;
import dev.vality.ccreporter.fixture.ReportRequestFixtures;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ThriftJsonCodecTest {

    private final ThriftJsonCodec codec = new ThriftJsonCodec(new JacksonConfig().objectMapper());

    @Test
    void serializesNestedUnionAndEnumsAsReadableJson() {
        var request = ReportRequestFixtures.payments("thrift-json-codec-1", new TimeRange(
                "2026-01-01T00:00:00Z",
                "2026-01-02T00:00:00Z"
        ));
        request.setReportType(ReportType.payments);
        request.setFileType(FileType.csv);
        request.setQuery(ReportQuery.payments(request.getQuery().getPayments()
                .setPartyIds(List.of("party-1"))
                .setFilter(new PaymentsSearchFilter().setShopTerm("shop-term"))));

        var json = codec.serialize(request);

        assertThat(json)
                .contains("\"report_type\":\"payments\"")
                .contains("\"file_type\":\"csv\"")
                .contains("\"query\":{\"payments\"")
                .contains("\"time_range\"")
                .contains("\"party_ids\"")
                .contains("\"shop_term\"")
                .doesNotContain("fieldMetaData")
                .doesNotContain("setField_")
                .doesNotContain("value_");
    }

    @Test
    void roundTripsStructWithNestedUnionAndEnums() {
        var request = ReportRequestFixtures.withdrawals("thrift-json-codec-2", new TimeRange(
                "2026-02-01T00:00:00Z",
                "2026-02-02T00:00:00Z"
        ));

        var restored = codec.deserialize(codec.serialize(request), CreateReportRequest.class);

        assertThat(restored).isEqualTo(request);
    }
}
