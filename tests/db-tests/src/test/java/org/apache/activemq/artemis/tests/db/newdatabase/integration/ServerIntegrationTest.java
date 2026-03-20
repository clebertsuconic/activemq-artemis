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
package org.apache.activemq.artemis.tests.db.newdatabase.integration;

import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.db.newdatabase.statements.AbstractStatementTest;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class ServerIntegrationTest extends AbstractStatementTest {

   @TestTemplate
   public void testSimpleTXSend() throws Exception {

      ActiveMQServer server = createServer(true, configuration);

      server.start();

      int nMessages = 0;

      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (javax.jms.Connection connection = factory.createConnection()) {
         try (Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE)) {
            MessageProducer producer = session.createProducer(session.createQueue("TEST"));
            for (int i = 0; i < 10; i++) {
               producer.send(session.createTextMessage("test: " + i));
               nMessages++;
            }
            session.commit();
         }

         checkMessageCounts(nMessages, false);
      }
   }

   @TestTemplate
   public void testCreateAddress() throws Exception {
      ActiveMQServer server = createServer(true, configuration);
      server.start();
      server.addAddressInfo(new AddressInfo("test").addRoutingType(RoutingType.ANYCAST));
      server.addAddressInfo(new AddressInfo("test1").addRoutingType(RoutingType.ANYCAST));
      Thread.sleep(1000);
      server.stop();

      server = createServer(true, configuration);
      server.start();

      assertNotNull(server.getAddressInfo(SimpleString.of("test")));
      assertNotNull(server.getAddressInfo(SimpleString.of("test1")));
      server.stop();
   }

   @TestTemplate
   public void testSendAndRestart() throws Exception {
      ActiveMQServer server = createServer(true, configuration);

      server.start();

      int nMessages = 0;

      String[] protocols = {"CORE", "AMQP", "OPENWIRE"};
      for (String p : protocols) {
         ConnectionFactory factory = CFUtil.createConnectionFactory(p, "tcp://localhost:61616");
         try (javax.jms.Connection connection = factory.createConnection()) {
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
               MessageProducer producer = session.createProducer(session.createQueue("TEST"));
               for (int i = 0; i < 10; i++) {
                  producer.send(session.createTextMessage("test: " + p));
                  nMessages++;
               }
            }

            checkMessageCounts(nMessages, false);

            try (Session session = connection.createSession(true, Session.SESSION_TRANSACTED)) {
               int beforeCommit = nMessages;
               MessageProducer producer = session.createProducer(session.createQueue("TEST"));
               for (int i = 0; i < 10; i++) {
                  producer.send(session.createTextMessage("test: " + p));
                  nMessages++;
               }
               checkMessageCounts(beforeCommit, false);
               session.commit();
               checkMessageCounts(nMessages, false);
            }
         }

         server.stop();
         server.start();

         try (javax.jms.Connection connection = factory.createConnection()) {
            try (Session session = connection.createSession(true, Session.SESSION_TRANSACTED)) {
               connection.start();
               MessageConsumer consumer = session.createConsumer(session.createQueue("TEST"));
               for (int i = 0; i < nMessages; i++) {
                  assertNotNull(consumer.receive(5000));
               }

               checkMessageCounts(nMessages, false);
               session.commit();
               checkMessageCounts(0, true);
               nMessages = 0;
            }
         }
      }

   }
}