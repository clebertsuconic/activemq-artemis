/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.server;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.Test;

public class IOCriticalErrorListenerTest extends ActiveMQTestBase {
   protected ActiveMQServer server;

   @Test
   public void simpleTest() throws Exception {
      server = createServer(true, createDefaultNettyConfig());
      final CountDownLatch latch = new CountDownLatch(1);
      server.registerIOCriticalErrorListener((code, message, file) -> latch.countDown());
      server.start();
      server.getStorageManager().criticalError(new Exception("Fake Failure"));
      assertTrue(latch.await(3000, TimeUnit.MILLISECONDS));
   }
}
