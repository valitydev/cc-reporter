package dev.vality.ccreporter.ingestion.dominant;

import dev.vality.ccreporter.dao.DominantLookupDao;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;

import static dev.vality.ccreporter.dao.DominantLookupDao.LookupType.*;

@Service
@RequiredArgsConstructor
public class DominantLookupIngestionService {

    private final DominantLookupDao dominantLookupDao;

    @Transactional
    public void handleCommits(List<HistoricalCommit> commits) {
        commits.stream()
                .sorted(Comparator.comparingLong(HistoricalCommit::getVersion))
                .forEach(this::handleCommit);
    }

    private void handleCommit(HistoricalCommit commit) {
        if (commit == null || commit.getOps() == null) {
            return;
        }
        for (FinalOperation operation : commit.getOps()) {
            if (operation.isSetInsert()) {
                upsertObject(operation.getInsert().getObject(), commit.getVersion());
            } else if (operation.isSetUpdate()) {
                upsertObject(operation.getUpdate().getObject(), commit.getVersion());
            } else if (operation.isSetRemove()) {
                removeObject(operation.getRemove().getRef(), commit.getVersion());
            }
        }
    }

    private void upsertObject(DomainObject object, long version) {
        if (object == null) {
            return;
        }
        if (object.isSetShopConfig()) {
            var shop = object.getShopConfig();
            if (shop.getRef() != null && shop.getData() != null) {
                dominantLookupDao.upsert(
                        SHOP,
                        shop.getRef().getId(),
                        shop.getData().getName(),
                        version,
                        false
                );
            }
            return;
        }
        if (object.isSetProvider()) {
            var provider = object.getProvider();
            if (provider.getRef() != null && provider.getData() != null) {
                dominantLookupDao.upsert(
                        PROVIDER,
                        String.valueOf(provider.getRef().getId()),
                        provider.getData().getName(),
                        version,
                        false
                );
            }
            return;
        }
        if (object.isSetTerminal()) {
            var terminal = object.getTerminal();
            if (terminal.getRef() != null && terminal.getData() != null) {
                dominantLookupDao.upsert(
                        TERMINAL,
                        String.valueOf(terminal.getRef().getId()),
                        terminal.getData().getName(),
                        version,
                        false
                );
            }
            return;
        }
        if (object.isSetWalletConfig()) {
            var wallet = object.getWalletConfig();
            if (wallet.getRef() != null && wallet.getData() != null) {
                dominantLookupDao.upsert(
                        WALLET,
                        wallet.getRef().getId(),
                        wallet.getData().getName(),
                        version,
                        false
                );
            }
        }
    }

    private void removeObject(Reference reference, long version) {
        if (reference == null) {
            return;
        }
        if (reference.isSetShopConfig()) {
            dominantLookupDao.upsert(
                    SHOP,
                    reference.getShopConfig().getId(),
                    null,
                    version,
                    true
            );
            return;
        }
        if (reference.isSetProvider()) {
            dominantLookupDao.upsert(
                    PROVIDER,
                    String.valueOf(reference.getProvider().getId()),
                    null,
                    version,
                    true
            );
            return;
        }
        if (reference.isSetTerminal()) {
            dominantLookupDao.upsert(
                    TERMINAL,
                    String.valueOf(reference.getTerminal().getId()),
                    null,
                    version,
                    true
            );
            return;
        }
        if (reference.isSetWalletConfig()) {
            dominantLookupDao.upsert(
                    WALLET,
                    reference.getWalletConfig().getId(),
                    null,
                    version,
                    true
            );
        }
    }
}
