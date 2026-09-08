/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.SenderError;
import io.questdb.client.std.FilesFacade;

import java.util.UUID;

/** Immutable configuration for synchronous schema-rejection preservation. */
public final class SchemaPreserver {
    private final String epoch;
    private final FilesFacade ff;
    private final String slotDir;
    private final String slotId;

    public SchemaPreserver(FilesFacade ff, String slotDir, String slotId, String epoch) {
        this.ff = ff;
        this.slotDir = slotDir;
        this.slotId = slotId;
        this.epoch = epoch;
    }

    public SenderError findRecoveredOrphanReport(long fromFsn, long toFsn) {
        return RejectedMiniSlotArchive.findOverlapping(
                ff, slotDir, slotId, epoch, fromFsn, toFsn);
    }

    /**
     * Preserves the sealed range on the calling thread. A preceding failed
     * publication may have left its uniquely named temporary tree behind, so
     * clean only this slot and epoch's known temporary scope before retrying.
     */
    public RejectedMiniSlotArchive.Result preserve(
            CursorSendEngine engine,
            SenderError error,
            byte[] dictionaryEntries,
            int dictionaryCount
    ) {
        RejectedMiniSlotArchive.cleanupTemporaryDirectories(ff, slotDir, slotId, epoch);
        return RejectedMiniSlotArchive.preserveSnapshot(
                ff, engine, dictionaryEntries, dictionaryCount,
                slotDir, slotId, epoch, error);
    }

    /** Build-time writability and directory-durability probe. */
    public static void probeDestination(FilesFacade ff, String slotDir) {
        probeDirectory(ff, slotDir + "/rejected");
    }

    /** Probes an explicitly configured destination without assuming a slot layout. */
    public static void probeDirectory(FilesFacade ff, String directory) {
        if (!ff.exists(directory) && ff.mkdir(directory, 448) != 0) {
            throw new SfOperationalException("could not create schema preservation destination " + directory);
        }
        String probe = directory + "/.probe-" + UUID.randomUUID();
        int fd = ff.openRWExclusive(probe);
        try {
            if (fd < 0 || ff.fsync(fd) != 0) {
                throw new SfOperationalException("schema preservation destination is not writable " + directory);
            }
        } finally {
            if (fd >= 0) ff.close(fd);
            ff.remove(probe);
        }
        if (ff.fsyncDir(directory) != 0) {
            throw new SfOperationalException("could not sync schema preservation destination " + directory);
        }
    }
}
