package dev.vality.ccreporter.fixture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

class RealFixtureCoverageTest {

    @Test
    void paymentCollectionFixtureCoversAllPaymentResources() throws IOException {
        assertThat(RealPaymentIngestionEventFixtures.collectionResourceNames().stream().sorted().toList())
                .containsExactlyElementsOf(resourceFiles("payments"));
    }

    @Test
    void withdrawalCollectionFixtureCoversAllWithdrawalResources() throws IOException {
        assertThat(RealWithdrawalIngestionEventFixtures.collectionResourceNames().stream().sorted().toList())
                .containsExactlyElementsOf(resourceFiles("withdrawals"));
    }

    private static java.util.List<String> resourceFiles(String directory) throws IOException {
        try (var paths = Files.walk(Path.of("src/test/resources", directory), 1)) {
            return paths
                    .filter(Files::isRegularFile)
                    .map(path -> "payments".equals(directory)
                            ? "payments/" + path.getFileName()
                            : "withdrawals/" + path.getFileName())
                    .sorted(Comparator.naturalOrder())
                    .toList();
        }
    }
}
