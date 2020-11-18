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

package org.apache.activemq.artemis.tests.smoke.paging;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.messages.Consumer;
import org.apache.activemq.artemis.cli.commands.messages.Producer;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplicatedPageCounterTest extends SmokeTestBase {

   private static final String JMX_SERVER_HOSTNAME = "localhost";

   public static final String SERVER_NAME_0 = "replicated-static0";
   public static final String SERVER_NAME_1 = "replicated-static1";

   private static final int JMX_SERVER0_PORT = 10099;
   private static final int JMX_SERVER1_PORT = 11099;

   private SimpleString ADDRESS = new SimpleString("ReplicatedPageCounterTest");

   Process server0;

   @Before
   public void before() throws Exception {
      disableCheckThread();
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      server0 = startServer(SERVER_NAME_0, 0, 30000);
      startServer(SERVER_NAME_1, 0, 30000);
      //startServer(SERVER_NAME_1, 0, 30000);
   }


   @Test
   public void testPaging() throws Exception {
      String protocol = "CORE";
      // Without this, the RMI server would bind to the default interface IP (the user's local IP mostly)
      System.setProperty("java.rmi.server.hostname", JMX_SERVER_HOSTNAME);

      final int MESSAGE_COUNT = 20000;

      MBeanServerConnection mBeanServerConnection = connectJMX(JMX_SERVER1_PORT);

      ConnectionFactory factory = createConnectionFactory(protocol, "tcp://localhost:61616");
      Connection connection = factory.createConnection();
      addCloseable(connection);
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer producer = session.createProducer(session.createQueue(ADDRESS.toString()));
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < 1000; i++) {
         builder.append('a' + (i %20));
      }
      String text = builder.toString();
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         TextMessage message = session.createTextMessage(text);
         producer.send(message);
         if (i % 200 == 0) {
            session.commit();
         }
      }
      session.commit();

      connection.start();

      MessageConsumer consumer = session.createConsumer(session.createQueue(ADDRESS.toString()));
      for (int i = 0; i < MESSAGE_COUNT / 2; i++) {
         Message message = consumer.receive(5000);
         if (i % 200 == 0) {
            session.commit();
         }
      }
      session.commit();
      server0.destroyForcibly();

      String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
      ActiveMQServerControl activeMQServerControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getActiveMQServerObjectName(), ActiveMQServerControl.class, false);

      Wait.assertTrue(activeMQServerControl::isActive);

      QueueControl queueControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getQueueObjectName(ADDRESS, ADDRESS, RoutingType.ANYCAST), QueueControl.class, false);
      Assert.assertEquals(MESSAGE_COUNT / 2, queueControl.getMessageCount());


   }

   protected MBeanServerConnection connectJMX(int port) throws Exception {
      // I don't specify both ports here manually on purpose. See actual RMI registry connection port extraction below.
      String urlString = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + port + "/jmxrmi";

      JMXServiceURL url = new JMXServiceURL(urlString);
      JMXConnector jmxConnector = null;

      for (int retry = 0; retry < 20; retry++) {
         try {
            jmxConnector = JMXConnectorFactory.connect(url);
            addCloseable(jmxConnector);
            System.out.println("Successfully connected to: " + urlString);
         } catch (Exception e) {
            jmxConnector = null;
            e.printStackTrace();
            Thread.sleep(500);
         }
      }

      Assert.assertNotNull(jmxConnector);

      MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
      return mBeanServerConnection;
   }

}
