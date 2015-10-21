/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.failover;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.apache.activemq.broker.artemiswrapper.OpenwireArtemisBaseTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FailoverClusterTest extends OpenwireArtemisBaseTest {

   private static final int NUMBER = 10;
   private String clientUrl;

   private final List<ActiveMQConnection> connections = new ArrayList<ActiveMQConnection>();
   EmbeddedJMS server1;
   EmbeddedJMS server2;


   @Before
   public void setUp() throws Exception {
      Configuration config1 = createConfig(1);
      Configuration config2 = createConfig(2);

      deployClusterConfiguration(config1, 2);
      deployClusterConfiguration(config2, 1);

      server1 = new EmbeddedJMS().setConfiguration(config1).setJmsConfiguration(new JMSConfigurationImpl());
      server2 = new EmbeddedJMS().setConfiguration(config2).setJmsConfiguration(new JMSConfigurationImpl());

      server1.start();
      server2.start();

      Assert.assertTrue(server1.waitClusterForming(100, TimeUnit.MILLISECONDS, 20, 2));
      Assert.assertTrue(server2.waitClusterForming(100, TimeUnit.MILLISECONDS, 20, 2));

      clientUrl = "failover://(" + newURI(1) + "," + newURI(2) + ")";
   }

   @After
   public void tearDown() throws Exception {
      for (Connection c : connections) {
         c.close();
      }
      server1.stop();
      server2.stop();
   }


   @Test
   public void testClusterConnectedAfterClients() throws Exception {
      createClients();
      Set<String> set = new HashSet<String>();
      for (ActiveMQConnection c : connections) {
         System.out.println("======> adding address: " + c.getTransportChannel().getRemoteAddress());
         set.add(c.getTransportChannel().getRemoteAddress());
      }
      System.out.println("============final size: " + set.size());
      Assert.assertTrue(set.size() > 1);
   }

   @Test
   public void testClusterURIOptionsStrip() throws Exception {
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
      Connection connection = connectionFactory.createConnection();


      createClients();
      Set<String> set = new HashSet<String>();
      for (ActiveMQConnection c : connections) {
         set.add(c.getTransportChannel().getRemoteAddress());
      }
      Assert.assertTrue(set.size() > 1);
   }

   @Test
   public void testClusterConnectedBeforeClients() throws Exception {

      createClients();
      server1.stop();

      URI brokerBURI = new URI(newURI(2));
      for (ActiveMQConnection c : connections) {
         String addr = c.getTransportChannel().getRemoteAddress();
         Assert.assertTrue(addr.indexOf("" + brokerBURI.getPort()) > 0);
      }
   }


   @SuppressWarnings("unused")
   protected void createClients() throws Exception {
      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(clientUrl);
      for (int i = 0; i < NUMBER; i++) {
         System.out.println("*****create connection using url: " + clientUrl);
         ActiveMQConnection c = (ActiveMQConnection) factory.createConnection();
         System.out.println("got connection, starting it ...");
         c.start();
         System.out.println("******Started");
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = s.createQueue(getClass().getName());
         MessageConsumer consumer = s.createConsumer(queue);
         connections.add(c);
      }
   }
}
