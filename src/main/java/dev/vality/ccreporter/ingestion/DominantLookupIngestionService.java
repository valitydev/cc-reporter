package dev.vality.ccreporter.ingestion;

import dev.vality.ccreporter.dao.DisplayNameLookupDao;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;

@Service
public class DominantLookupIngestionService {

    private final DisplayNameLookupDao displayNameLookupDao;

    public DominantLookupIngestionService(DisplayNameLookupDao displayNameLookupDao) {
        this.displayNameLookupDao = displayNameLookupDao;
    }

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
                displayNameLookupDao.upsertShop(shop.getRef().getId(), shop.getData().getName(), version);
            }
            return;
        }
        if (object.isSetProvider()) {
            var provider = object.getProvider();
            if (provider.getRef() != null && provider.getData() != null) {
                displayNameLookupDao.upsertProvider(
                        String.valueOf(provider.getRef().getId()),
                        provider.getData().getName(),
                        version
                );
            }
            return;
        }
        if (object.isSetTerminal()) {
            var terminal = object.getTerminal();
            if (terminal.getRef() != null && terminal.getData() != null) {
                displayNameLookupDao.upsertTerminal(
                        String.valueOf(terminal.getRef().getId()),
                        terminal.getData().getName(),
                        version
                );
            }
            return;
        }
        if (object.isSetWalletConfig()) {
            var wallet = object.getWalletConfig();
            if (wallet.getRef() != null && wallet.getData() != null) {
                displayNameLookupDao.upsertWallet(wallet.getRef().getId(), wallet.getData().getName(), version);
            }
        }
    }

    private void removeObject(Reference reference, long version) {
        if (reference == null) {
            return;
        }
        if (reference.isSetShopConfig()) {
            displayNameLookupDao.deleteShop(reference.getShopConfig().getId(), version);
            return;
        }
        if (reference.isSetProvider()) {
            displayNameLookupDao.deleteProvider(String.valueOf(reference.getProvider().getId()), version);
            return;
        }
        if (reference.isSetTerminal()) {
            displayNameLookupDao.deleteTerminal(String.valueOf(reference.getTerminal().getId()), version);
            return;
        }
        if (reference.isSetWalletConfig()) {
            displayNameLookupDao.deleteWallet(reference.getWalletConfig().getId(), version);
        }
    }
}
