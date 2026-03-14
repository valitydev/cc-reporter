package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.ingestion.dominant.DominantLookupIngestionService;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.DominantCommitFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

class DominantLookupIngestionIntegrationTest extends AbstractReportingIntegrationTest {

    @Autowired
    private DominantLookupIngestionService dominantLookupIngestionService;

    @Test
    void dominantLookupUpdatesAreMonotonicAndTombstonesBlockStaleReinsert() {
        dominantLookupIngestionService.handleCommits(java.util.List.of(
                DominantCommitFixtures.insertCommit(10L),
                DominantCommitFixtures.removeShopCommit(12L),
                DominantCommitFixtures.updateShopCommit(11L, "Stale Shop"),
                DominantCommitFixtures.updateProviderCommit(13L, "Provider New")
        ));

        var shop = jdbcTemplate.queryForMap(
                """
                        SELECT shop_name, dominant_version_id, deleted
                        FROM ccr.shop_lookup
                        WHERE shop_id = 'shop-lookup'
                        """
        );
        var provider = jdbcTemplate.queryForMap(
                """
                        SELECT provider_name, dominant_version_id, deleted
                        FROM ccr.provider_lookup
                        WHERE provider_id = '1001'
                        """
        );

        assertThat(shop.get("shop_name")).isNull();
        assertThat(shop.get("dominant_version_id")).isEqualTo(12L);
        assertThat(shop.get("deleted")).isEqualTo(true);
        assertThat(provider.get("provider_name")).isEqualTo("Provider New");
        assertThat(provider.get("dominant_version_id")).isEqualTo(13L);
        assertThat(provider.get("deleted")).isEqualTo(false);
    }
}
