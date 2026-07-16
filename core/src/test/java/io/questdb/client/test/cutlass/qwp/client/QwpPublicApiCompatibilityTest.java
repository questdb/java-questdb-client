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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.ClientTlsConfiguration;
import io.questdb.client.Sender;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QwpPublicApiCompatibilityTest {

    @Test
    public void testBackgroundDrainerLegacyConstructorDescriptorIsPreserved() throws Exception {
        Constructor<BackgroundDrainer> constructor = BackgroundDrainer.class.getConstructor(
                String.class,
                long.class,
                long.class,
                CursorWebSocketSendLoop.ReconnectFactory.class,
                long.class,
                long.class,
                long.class,
                boolean.class,
                long.class,
                int.class,
                long.class
        );
        assertTrue(Modifier.isPublic(constructor.getModifiers()));
    }

    @Test
    public void testQwpWebSocketSenderLegacyConnectDescriptorIsPreserved() throws Exception {
        Method method = QwpWebSocketSender.class.getMethod(
                "connect",
                List.class,
                ClientTlsConfiguration.class,
                int.class,
                int.class,
                long.class,
                String.class,
                boolean.class,
                CursorSendEngine.class,
                long.class,
                long.class,
                long.class,
                long.class,
                Sender.InitialConnectMode.class,
                SenderErrorHandler.class,
                int.class,
                long.class,
                long.class,
                int.class,
                SenderConnectionListener.class,
                int.class,
                int.class,
                long.class
        );
        assertEquals(QwpWebSocketSender.class, method.getReturnType());
        assertTrue(Modifier.isPublic(method.getModifiers()));
        assertTrue(Modifier.isStatic(method.getModifiers()));
    }
}
