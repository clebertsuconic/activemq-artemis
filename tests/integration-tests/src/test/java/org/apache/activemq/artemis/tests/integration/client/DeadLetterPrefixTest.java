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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.DeadLetterAddressRoutingType;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DeadLetterPrefixTest extends Assert {
   private EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
   private ConfigurationImpl configuration;
   private ServerLocator locator = ActiveMQClient.createServerLocator("vm://0");

   public DeadLetterPrefixTest() throws Exception {
   }

   @Before
   public void setup() throws Exception {
      configuration = new ConfigurationImpl();
      configuration.addAcceptorConfiguration("in-vm", "vm://0");
      configuration.setSecurityEnabled(false);
      configuration.setPersistenceEnabled(false);
   }

   @After
   public void tearDown() throws Exception {
      embeddedActiveMQ.stop();
   }

   @Test
   public void testNonJmsNoQueueAutoCreation() throws Exception {
      configuration.addAddressConfiguration(new CoreAddressConfiguration().setName("a1")
              .addQueueConfiguration(new CoreQueueConfiguration().setName("q1").setRoutingType(RoutingType.ANYCAST).setAddress("a1")));
      configuration.addAddressesSetting("#", new AddressSettings().setDeadLetterAddressPrefix(new SimpleString("DLA.")).setMaxDeliveryAttempts(1));
      configuration.addAddressesSetting("DLA.#", new AddressSettings().setAutoCreateAddresses(true).setAutoCreateQueues(false));

      embeddedActiveMQ.setConfiguration(configuration);
      embeddedActiveMQ.start();

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, false);

      ClientProducer producer = session.createProducer("a1");
      producer.send(session.createMessage(false).writeBodyBufferString("Test content"));

      assertEquals("Test content", getNextMessage(factory, "q1", true));
      assertNotNull(embeddedActiveMQ.getActiveMQServer().getAddressInfo(new SimpleString("DLA.a1")));
      assertEquals(0, embeddedActiveMQ.getActiveMQServer().getTotalMessageCount());
   }

   @Test
   public void testNonJms() throws Exception {
      configuration.addAddressConfiguration(new CoreAddressConfiguration().setName("a1")
              .addQueueConfiguration(new CoreQueueConfiguration().setName("q1").setRoutingType(RoutingType.ANYCAST).setAddress("a1")));
      configuration.addAddressesSetting("#", new AddressSettings().setDeadLetterAddressPrefix(new SimpleString("DLA.")).setMaxDeliveryAttempts(1));
      configuration.addAddressesSetting("DLA.#", new AddressSettings().setAutoCreateAddresses(true).setAutoCreateQueues(true));

      embeddedActiveMQ.setConfiguration(configuration);
      embeddedActiveMQ.start();

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, false);

      ClientProducer producer = session.createProducer("a1");
      producer.send(session.createMessage(false).writeBodyBufferString("Test content"));

      assertEquals("Test content", getNextMessage(factory, "q1", true));
      assertEquals("Test content", getNextMessage(factory, "DLA.q1", false));

      assertEquals(0, embeddedActiveMQ.getActiveMQServer().getTotalMessageCount());
   }

   @Test
   public void testNonJmsWithAnycastWitMultipleQueues() throws Exception {
      configuration.addAddressConfiguration(new CoreAddressConfiguration().setName("a1")
              .addQueueConfiguration(new CoreQueueConfiguration().setName("q1").setRoutingType(RoutingType.ANYCAST).setAddress("a1"))
              .addQueueConfiguration(new CoreQueueConfiguration().setName("q2").setRoutingType(RoutingType.ANYCAST).setAddress("a1"))
      );
      configuration.addAddressesSetting("#", new AddressSettings().setDeadLetterAddressPrefix(new SimpleString("DLA.")).setDeadLetterAddressAutoCreateRoutingType(DeadLetterAddressRoutingType.CORRESPONDING_QUEUE).setMaxDeliveryAttempts(1).setQueuePrefetch(1));
      configuration.addAddressesSetting("DLA.#", new AddressSettings().setAutoCreateAddresses(true).setAutoCreateQueues(true).setMaxDeliveryAttempts(1).setQueuePrefetch(1));

      embeddedActiveMQ.setConfiguration(configuration);
      embeddedActiveMQ.start();

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, false);

      ClientProducer producer = session.createProducer("a1");
      producer.send(session.createMessage(false).writeBodyBufferString("Test content1"));
      producer.send(session.createMessage(false).writeBodyBufferString("Test content2"));
      producer.send(session.createMessage(false).writeBodyBufferString("Test content3"));
      producer.send(session.createMessage(false).writeBodyBufferString("Test content4"));
      producer.send(session.createMessage(false).writeBodyBufferString("Test content5"));
      session.commit();
      session.close();

      assertEquals("Test content1", getNextMessage(factory, "q1", true));
      assertEquals("Test content2", getNextMessage(factory, "q2", true));
      assertEquals("Test content3", getNextMessage(factory, "q1", true));
      assertEquals("Test content4", getNextMessage(factory, "q2", false));
      assertEquals("Test content5", getNextMessage(factory, "q1", true));

      assertEquals("Test content1", getNextMessage(factory, "DLA.q1", false));
      assertEquals("Test content2", getNextMessage(factory, "DLA.q2", false));
      assertEquals("Test content3", getNextMessage(factory, "DLA.q1", false));
      assertEquals("Test content5", getNextMessage(factory, "DLA.q1", false));

      assertEquals(0, embeddedActiveMQ.getActiveMQServer().getTotalMessageCount());
   }

   private String getNextMessage(ClientSessionFactory sessionFactory, String queueName, boolean rollback) throws ActiveMQException {
      String body = null;

      ClientSession session = sessionFactory.createSession(false, true, false);
      session.start();

      ClientConsumer consumer = session.createConsumer(queueName);
      ClientMessage message = consumer.receive(1000);

      if (message != null) {
         body = message.getBodyBuffer().readString();
         message.acknowledge();
      }

      if (rollback) {
         session.rollback();
      } else {
         session.commit();
      }

      consumer.close();
      session.close();

      return body;
   }
}
