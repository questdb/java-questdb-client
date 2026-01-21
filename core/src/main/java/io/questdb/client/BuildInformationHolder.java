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

package io.questdb.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class BuildInformationHolder implements BuildInformation {
    private static final String UNKNOWN = "unknown";
    private final String swVersion;

    public BuildInformationHolder() {
        this(BuildInformationHolder.class);
    }

    public BuildInformationHolder(Class<?> clazz) {
        String swVersion;
        try {
            final Attributes manifestAttributes = getManifestAttributes(clazz);
            swVersion = getAttr(manifestAttributes, "QuestDB-Client-Version", "[DEVELOPMENT]");
        } catch (IOException e) {
            swVersion = UNKNOWN;
        }
        this.swVersion = swVersion;
    }

    public BuildInformationHolder(String swVersion) {
        this.swVersion = swVersion;
    }

    @Override
    public String getSwVersion() {
        return swVersion;
    }

    private static String getAttr(final Attributes manifestAttributes, String attributeName, String defaultValue) {
        final String value = manifestAttributes.getValue(attributeName);
        return value != null ? value : defaultValue;
    }

    private static Attributes getManifestAttributes(Class<?> clazz) throws IOException {
        InputStream is = clazz.getResourceAsStream("/META-INF/MANIFEST.MF");
        if (is != null) {
            try {
                final Attributes attributes = new Manifest(is).getMainAttributes();
                final String vendor = attributes.getValue("Implementation-Vendor-Id");
                if (vendor != null && vendor.contains("questdb")) {
                    return attributes;
                }
            } finally {
                is.close();
            }
        }
        return new Attributes();
    }
}