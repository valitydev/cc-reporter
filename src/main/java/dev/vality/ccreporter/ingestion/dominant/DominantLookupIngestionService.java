package dev.vality.ccreporter.ingestion.dominant;

import dev.vality.ccreporter.dao.DisplayNameLookupDao;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;

import static dev.vality.ccreporter.dao.DisplayNameLookupDao.LookupType.*;

@Service
@RequiredArgsConstructor
public class DominantLookupIngestionService {

    private final DisplayNameLookupDao displayNameLookupDao;

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
                displayNameLookupDao.upsert(
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
                displayNameLookupDao.upsert(
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
                displayNameLookupDao.upsert(
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
                displayNameLookupDao.upsert(
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
            displayNameLookupDao.upsert(
                    SHOP,
                    reference.getShopConfig().getId(),
                    null,
                    version,
                    true
            );
            return;
        }
        if (reference.isSetProvider()) {
            displayNameLookupDao.upsert(
                    PROVIDER,
                    String.valueOf(reference.getProvider().getId()),
                    null,
                    version,
                    true
            );
            return;
        }
        if (reference.isSetTerminal()) {
            displayNameLookupDao.upsert(
                    TERMINAL,
                    String.valueOf(reference.getTerminal().getId()),
                    null,
                    version,
                    true
            );
            return;
        }
        if (reference.isSetWalletConfig()) {
            displayNameLookupDao.upsert(
                    WALLET,
                    reference.getWalletConfig().getId(),
                    null,
                    version,
                    true
            );
        }
    }
}
