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

package io.questdb.client.azure.test;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Conformance test C24 (design/qwp-token-provider-spec.md, section 10) against the real Azure Identity library: how
 * each {@code azure_credential} mode reports a managed-identity endpoint that is down, and one that answers that
 * the identity is not assigned.
 * <p>
 * Each case runs in a child JVM ({@link AzureCredentialProbeMain}): MSAL takes the endpoint only from the process
 * environment, {@code AZURE_POD_IDENTITY_AUTHORITY_HOST}, so that is the only way to point it at a local
 * {@link ImdsStub} - and never at the real instance metadata service, which a CI runner on an Azure VM can reach.
 * The child inherits no other Azure Identity variable.
 */
public class AzureManagedIdentityLibraryTest {
    private static final long CHILD_TIMEOUT_MILLIS = 90_000;
    private static final long NEVER = -1;

    @Test(timeout = 120_000)
    public void testDiscoveryChainFailsFastAndWarnsOnce() throws Exception {
        // AZURE_TOKEN_CREDENTIALS=prod keeps developer tools out of the chain: environment, workload identity and
        // managed identity remain. The chain checks the endpoint once, without retrying, so a host whose endpoint
        // is down looks like one without a credential: permanent, as section 7.5 classifies it, and fast.
        try (ImdsStub stub = new ImdsStub(ImdsStub.Answer.TOKEN)) {
            Result r = run(stub, NEVER, "prod", "factory", "default");
            Assert.assertFalse(r.output, r.token);
            Assert.assertFalse(r.output, r.retryable);
            Assert.assertTrue(r.output, r.message.startsWith("no credential available in the Azure Identity chain"));
            Assert.assertTrue("the chain must not retry the endpoint: " + r.output, r.elapsedMillis < 10_000);
            Assert.assertEquals(r.output, 1, r.factoryWarnings);
        }
    }

    @Test(timeout = 120_000)
    public void testManagedIdentityReportsAnUnassignedIdentityAsPermanent() throws Exception {
        // a configuration error: a sync startup must fail fast, not retry it for the whole connect budget
        try (ImdsStub stub = new ImdsStub(ImdsStub.Answer.IDENTITY_NOT_FOUND)) {
            Result r = run(stub, 0, null, "factory", "managed_identity");
            Assert.assertFalse(r.output, r.token);
            Assert.assertFalse(r.output, r.retryable);
            Assert.assertEquals(r.output, "managed identity unavailable for " + AzureCredentialProbeMain.RESOURCE
                    + "/.default: the requested identity has not been assigned to this resource", r.message);
            Assert.assertTrue("the stub must have answered: " + r.output, stub.tokenRequests() >= 1);
            Assert.assertEquals(r.output, 0, r.factoryWarnings);
        }
    }

    @Test(timeout = 120_000)
    public void testManagedIdentityReportsAnUnreachableEndpointAsRetryable() throws Exception {
        // The endpoint never comes up. The library retries it with backoff for about 25 s and then gives up with a
        // generic error that is retryable, naming the refused connection. The source's bound outlasts those
        // retries, so this is the library's verdict, not the source's own timeout.
        try (ImdsStub stub = new ImdsStub(ImdsStub.Answer.TOKEN)) {
            Result r = run(stub, NEVER, null, "direct", "75000");
            Assert.assertFalse(r.output, r.token);
            Assert.assertTrue(r.output, r.retryable);
            Assert.assertTrue(r.output, r.message.contains("ConnectException"));
            Assert.assertEquals(r.output, 0, stub.tokenRequests());
        }
    }

    @Test(timeout = 120_000)
    public void testManagedIdentityRidesOutAnEndpointOutage() throws Exception {
        // the endpoint comes up 2 s after the fetch starts: the library's own retries reach it within that fetch
        try (ImdsStub stub = new ImdsStub(ImdsStub.Answer.TOKEN)) {
            Result r = run(stub, 2_000, null, "factory", "managed_identity");
            Assert.assertTrue(r.output, r.token);
            Assert.assertTrue("the fetch must have outlasted the outage: " + r.output, r.elapsedMillis >= 1_500);
            Assert.assertEquals(r.output, 1, stub.tokenRequests());
            Assert.assertEquals(r.output, 0, r.factoryWarnings);
        }
    }

    @Test(timeout = 120_000)
    public void testPlatformSelectorNarrowsTheDefaultAndSilencesTheWarning() throws Exception {
        // azure_credential=default honours Azure Identity's own selector: one credential, not a chain to warn about
        try (ImdsStub stub = new ImdsStub(ImdsStub.Answer.TOKEN)) {
            Result r = run(stub, 0, "ManagedIdentityCredential", "factory", "default");
            Assert.assertTrue(r.output, r.token);
            Assert.assertEquals(r.output, 0, r.factoryWarnings);
        }
    }

    private static void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        //noinspection ResultOfMethodCallIgnored
        file.delete();
    }

    /**
     * Runs {@link AzureCredentialProbeMain} against {@code stub}, which starts answering {@code upAfterMillis} after
     * the child starts fetching: 0 before the child starts, {@link #NEVER} not at all.
     */
    private static Result run(ImdsStub stub, long upAfterMillis, String selector, String... args) throws Exception {
        if (upAfterMillis == 0) {
            stub.start();
        }
        final File home = Files.createTempDirectory("qdb-azure-probe").toFile();
        try {
            final List<String> command = new ArrayList<>();
            command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
            // no developer-tool profile (Azure CLI, IDEs) can be found under an empty home
            command.add("-Duser.home=" + home.getAbsolutePath());
            command.add("-cp");
            command.add(System.getProperty("java.class.path"));
            command.add(AzureCredentialProbeMain.class.getName());
            Collections.addAll(command, args);
            final ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true);
            final Map<String, String> env = pb.environment();
            env.keySet().removeIf(key -> key.startsWith("AZURE_") || key.startsWith("IDENTITY_")
                    || key.startsWith("MSI_") || key.startsWith("IMDS_"));
            env.put("AZURE_POD_IDENTITY_AUTHORITY_HOST", stub.endpoint());
            env.put("AZURE_CONFIG_DIR", new File(home, ".azure").getAbsolutePath());
            if (selector != null) {
                env.put("AZURE_TOKEN_CREDENTIALS", selector);
            }
            final Process process = pb.start();
            final StringBuilder output = new StringBuilder();
            final Thread reader = new Thread(() -> {
                try (BufferedReader in = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        synchronized (output) {
                            output.append(line).append('\n');
                        }
                        if (upAfterMillis > 0 && line.equals("PROBE fetching")) {
                            startLater(stub, upAfterMillis);
                        }
                    }
                } catch (IOException ignore) {
                    // the child is gone
                }
            }, "probe-output");
            reader.setDaemon(true);
            reader.start();
            if (!process.waitFor(CHILD_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                reader.join(5_000);
                Assert.fail("the probe did not finish in " + CHILD_TIMEOUT_MILLIS + " ms:\n" + output);
            }
            reader.join(10_000);
            final String text;
            synchronized (output) {
                text = output.toString();
            }
            Assert.assertEquals("the probe failed:\n" + text, 0, process.exitValue());
            return new Result(text);
        } finally {
            deleteRecursively(home);
        }
    }

    private static void startLater(ImdsStub stub, long delayMillis) {
        final Thread starter = new Thread(() -> {
            try {
                Thread.sleep(delayMillis);
                stub.start();
            } catch (InterruptedException | IOException ignore) {
                // the test is over
            }
        }, "imds-stub-starter");
        starter.setDaemon(true);
        starter.start();
    }

    private static final class Result {
        final long elapsedMillis;
        final int factoryWarnings;
        final String message;
        final String output;
        final boolean retryable;
        final boolean token;

        Result(String output) {
            this.output = output;
            String message = null;
            long elapsed = -1;
            int warnings = -1;
            boolean retryable = false;
            boolean token = false;
            for (String line : output.split("\n")) {
                if (!line.startsWith("PROBE ")) {
                    continue;
                }
                final String fact = line.substring("PROBE ".length());
                if (fact.startsWith("message=")) {
                    message = fact.substring("message=".length());
                } else if (fact.startsWith("elapsedMs=")) {
                    elapsed = Long.parseLong(fact.substring("elapsedMs=".length()));
                } else if (fact.startsWith("factoryWarnings=")) {
                    warnings = Integer.parseInt(fact.substring("factoryWarnings=".length()));
                } else if (fact.startsWith("retryable=")) {
                    retryable = Boolean.parseBoolean(fact.substring("retryable=".length()));
                } else if (fact.startsWith("token=")) {
                    token = Boolean.parseBoolean(fact.substring("token=".length()));
                }
            }
            Assert.assertTrue("the probe reported no result:\n" + output, elapsed >= 0 && warnings >= 0);
            this.message = message == null ? "" : message;
            this.elapsedMillis = elapsed;
            this.factoryWarnings = warnings;
            this.retryable = retryable;
            this.token = token;
        }
    }
}
