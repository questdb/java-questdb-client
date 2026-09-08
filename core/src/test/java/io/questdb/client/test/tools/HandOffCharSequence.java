/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.client.test.tools;

/**
 * A {@link CharSequence} that swaps its contents the instant a full scan of it completes.
 * <p>
 * Models the hazard {@code HttpTokenProvider} explicitly invites: a provider that returns a reused
 * mutable buffer. Any reader that VALIDATES the sequence and then RE-READS it to build the header has a
 * window between those two reads, and a mutation landing in it passes the check and ships the mutated
 * bytes - a CR/LF among them - into an {@code Authorization} header. Handing off exactly when the first
 * scan finishes puts the mutation in that window deterministically, with no threads involved.
 * <p>
 * {@link #toString()} materialises whatever the sequence currently holds, which is what a
 * {@code StringSink}- or {@code StringBuilder}-backed buffer does. That is what makes the fix
 * observable: a reader that snapshots BEFORE validating gets the clean value, because the snapshot is
 * taken before any scan has triggered the hand-off; a reader that validates first and stringifies after
 * gets the spliced one.
 */
public final class HandOffCharSequence implements CharSequence {
    private final String spliced;
    private CharSequence current;
    private boolean handedOff;

    public HandOffCharSequence(String clean, String spliced) {
        this.current = clean;
        this.spliced = spliced;
    }

    @Override
    public char charAt(int index) {
        final char c = current.charAt(index);
        if (!handedOff && index == current.length() - 1) {
            handedOff = true;
            current = spliced;
        }
        return c;
    }

    @Override
    public int length() {
        return current.length();
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return current.subSequence(start, end);
    }

    @Override
    public String toString() {
        // what a StringBuilder-backed buffer does: materialise whatever it currently holds
        return current.toString();
    }
}
