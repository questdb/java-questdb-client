/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.std.Crc32c;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;

import java.util.UUID;

/** Durable identity for one lifecycle of an SF slot's FSN namespace. */
public final class SlotEpoch {
    public static final String FILE_NAME = ".slot-epoch";
    private static final int CRC_OFFSET = 28;
    private static final int FILE_SIZE = 32;
    private static final int MAGIC = 0x31455053; // SPE1 little-endian
    private static final int VERSION = 1;

    private SlotEpoch() {
    }

    /**
     * Opens or creates the slot epoch. The caller must hold the slot's
     * exclusive {@link SlotLock}; this method publishes a newly-created epoch
     * with file and directory durability before returning it.
     */
    public static String openOrCreate(FilesFacade ff, String slotDir) {
        return openOrCreate(ff, slotDir, false);
    }

    /**
     * Opens the epoch for a recovered FSN namespace, or replaces it when the
     * caller has proved that this is a fresh namespace.  The latter check is
     * required even though clean close normally removes the sidecar: a crash
     * or unlink failure after durable segment removal can leave only the old
     * epoch behind.
     */
    public static String openOrCreate(FilesFacade ff, String slotDir, boolean freshFsnNamespace) {
        String path = slotDir + "/" + FILE_NAME;
        if (ff.exists(path) && !freshFsnNamespace) {
            String epoch = read(ff, path);
            // Also completes a prior create whose rename succeeded but whose
            // directory barrier reported a transient failure.
            if (ff.fsyncDir(slotDir) != 0) {
                throw new SfOperationalException("could not sync durable slot epoch " + path);
            }
            return epoch;
        }
        UUID uuid = UUID.randomUUID();
        String value = uuid.toString();
        String temp = path + ".tmp-" + UUID.randomUUID();
        long mem = Unsafe.malloc(FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        int fd = -1;
        boolean published = false;
        try {
            Unsafe.getUnsafe().setMemory(mem, FILE_SIZE, (byte) 0);
            Unsafe.getUnsafe().putInt(mem, MAGIC);
            Unsafe.getUnsafe().putInt(mem + 4, VERSION);
            Unsafe.getUnsafe().putLong(mem + 8, uuid.getMostSignificantBits());
            Unsafe.getUnsafe().putLong(mem + 16, uuid.getLeastSignificantBits());
            Unsafe.getUnsafe().putInt(mem + CRC_OFFSET, Crc32c.update(Crc32c.INIT, mem, CRC_OFFSET));
            fd = ff.openRWExclusive(temp);
            if (fd < 0 || !ff.allocate(fd, FILE_SIZE)
                    || ff.write(fd, mem, FILE_SIZE, 0) != FILE_SIZE
                    || ff.fsync(fd) != 0) {
                throw new SfOperationalException("could not create durable slot epoch " + path);
            }
            ff.close(fd);
            fd = -1;
            if (freshFsnNamespace && ff.exists(path) && !ff.remove(path)) {
                throw new SfOperationalException("could not replace stale slot epoch " + path);
            }
            if (ff.rename(temp, path) != 0 || ff.fsyncDir(slotDir) != 0) {
                throw new SfOperationalException("could not publish durable slot epoch " + path);
            }
            published = true;
            return value;
        } finally {
            if (fd >= 0) {
                ff.close(fd);
            }
            Unsafe.free(mem, FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
            if (!published) {
                ff.remove(temp);
            }
        }
    }

    public static String read(FilesFacade ff, String path) {
        if (ff.length(path) != FILE_SIZE) {
            throw new UnreplayableSlotException("invalid slot epoch size " + path);
        }
        long mem = Unsafe.malloc(FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        int fd = ff.openRW(path);
        try {
            if (fd < 0 || ff.read(fd, mem, FILE_SIZE, 0) != FILE_SIZE
                    || Unsafe.getUnsafe().getInt(mem) != MAGIC
                    || Unsafe.getUnsafe().getInt(mem + 4) != VERSION
                    || Unsafe.getUnsafe().getInt(mem + CRC_OFFSET)
                    != Crc32c.update(Crc32c.INIT, mem, CRC_OFFSET)) {
                throw new UnreplayableSlotException("invalid slot epoch " + path);
            }
            return new UUID(Unsafe.getUnsafe().getLong(mem + 8),
                    Unsafe.getUnsafe().getLong(mem + 16)).toString();
        } finally {
            if (fd >= 0) {
                ff.close(fd);
            }
            Unsafe.free(mem, FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
