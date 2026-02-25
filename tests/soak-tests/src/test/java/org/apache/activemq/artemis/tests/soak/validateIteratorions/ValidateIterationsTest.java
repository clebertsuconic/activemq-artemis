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

package org.apache.activemq.artemis.tests.soak.validateIteratorions;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidateIterationsTest extends SoakTestBase {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME = "validate-iterations";

   private static final String QUEUE_NAME = "FailoverTestQueue";
   private Process liveServer;

   @BeforeAll
   public static void createServers() throws Exception {
      File serverLocation = getFileServerLocation(SERVER_NAME);
      deleteDirectory(serverLocation);

      HelperCreate cliCreateServer = helperCreate();
      cliCreateServer.setUseAIO(false).setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
      cliCreateServer.addArgs("--no-fsync");
      cliCreateServer.addArgs("--java-memory", "512M");
      cliCreateServer.createServer();

      FileUtil.findReplace(new File(serverLocation, "/etc/broker.xml"), "<max-size-messages>-1</max-size-messages>", " <max-size-messages>1000</max-size-messages>");

   }


   @Test
   public void testValidateIterations() throws Exception {
      liveServer = startServer(SERVER_NAME, 0, 5000);

      //Increase this value until the test causes an OOME
      //The amount will depend on the heap size
      int messageCount = 200000;
      String queueName = "simpleTest";

      ConnectionFactory factory = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         connection.start();

         Session session = connection.createSession(Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createQueue(queueName));
         TextMessage message = session.createTextMessage(RandomUtil.randomAlphaNumericString(1024 * 40));

         for (int i = 0; i < messageCount; i++) {
            producer.send(message);

            if (i % 1000 == 0) {
               logger.info("sent {} out of {}", i, messageCount);
               session.commit();
            }
         }
         session.commit();
      }



   }



}
