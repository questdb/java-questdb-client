/*******************************************************************************
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

package io.questdb.client.test;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tripwire against mutation-tooling edits leaking into shipped sources.
 * <p>
 * Commit {@code 88d6b792} accidentally captured a concurrent mutation-testing
 * edit that neutralized {@code SegmentRing.requestSyncBeforeRotation} to
 * {@code return false; // MUTANT: gate neutralized} -- injected into a shared
 * worktree between test validation and {@code git add}. The full suite stayed
 * green and only human re-reading caught it ({@code 71cfbe2e}). Mutation
 * tools mark their edits precisely so they can be found; this test makes that
 * marker a build failure instead of a code-review lottery ticket.
 */
public class SourceHygieneTest {

    private static final String MARKER = "MUTANT";

    @Test
    public void testNoMutationToolMarkersInMainSources() throws IOException {
        // Surefire runs with the module directory (core/) as cwd.
        Path root = Paths.get("src", "main", "java");
        if (!Files.isDirectory(root)) {
            root = Paths.get("core", "src", "main", "java");
        }
        assertTrue("main source root not found from " + Paths.get("").toAbsolutePath()
                + " -- fix the path resolution rather than skipping the tripwire",
                Files.isDirectory(root));

        final List<String> offenders = new ArrayList<>();
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(".java")) {
                    List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
                    for (int i = 0; i < lines.size(); i++) {
                        if (lines.get(i).contains(MARKER)) {
                            offenders.add(file + ":" + (i + 1) + "  " + lines.get(i).trim());
                        }
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        assertTrue("mutation-tool markers must never reach main sources "
                        + "(a neutralized durability gate shipped exactly this way in 88d6b792); "
                        + "offending lines:\n" + String.join("\n", offenders),
                offenders.isEmpty());
    }
}
