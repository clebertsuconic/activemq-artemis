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
package org.apache.activemq.artemis.tests.integration.jms.cluster;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBridgePlugin;
import org.apache.activemq.artemis.tests.util.JMSClusteredExpiryTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.jms.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class StoreForwardExpiryTest extends JMSClusteredExpiryTestBase {

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      //todo fix if needed
      super.setUp();
      jmsServer1.getActiveMQServer().setIdentity("Server 1");
      jmsServer2.getActiveMQServer().setIdentity("Server 2");
   }

   @Override
   protected boolean enablePersistence() {
      return true;
   }

   @Override
   protected Configuration createConfigServer(final int source, final int destination) throws Exception {
      Configuration configuration = super.createConfigServer(source, destination);
      configuration.getAddressSettings().get("#").setAutoCreateExpiryResources(true).setExpiryQueuePrefix(SimpleString.of("")).setExpiryQueueSuffix(SimpleString.of(".IAMEXPIRED"));

      return configuration;
   }

   @Test
   public void testAutoCreateExpiry() throws Exception {
      server1.getAddressSettingsRepository().getMatch("#").setAutoCreateQueues(true).setAutoCreateAddresses(true).setRedistributionDelay(0);
      server2.getAddressSettingsRepository().getMatch("#").setAutoCreateQueues(true).setAutoCreateAddresses(true).setRedistributionDelay(0);

      CountDownLatch inDeliver = new CountDownLatch(1);
      CountDownLatch doDeliver = new CountDownLatch(1);
      server1.registerBrokerPlugin(new ActiveMQServerBridgePlugin() {
         @Override
         public void beforeDeliverBridge(Bridge bridge, MessageReference ref) throws ActiveMQException {
            inDeliver.countDown();
            try {
               doDeliver.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
            ActiveMQServerBridgePlugin.super.beforeDeliverBridge(bridge, ref);
         }
      });

      Connection conn1 = cf1.createConnection();
      Connection conn2 = cf2.createConnection();
      conn1.start();
      conn2.start();

      try {

         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod1 = session1.createProducer(ActiveMQJMSClient.createQueue("myQueue"));

         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
         prod1.setTimeToLive(TimeUnit.SECONDS.toMillis(2));
         for(int i = 0; i < 10 ; i++) {
            prod1.send(session1.createTextMessage("Message " + i));
         }

         MessageConsumer cons2 = session2.createConsumer(ActiveMQJMSClient.createQueue("myQueue"));

         // Stop server2
         assertTrue(inDeliver.await(6, TimeUnit.SECONDS));
         server2.stop(true);
         cons2.close();
         doDeliver.countDown();
         TimeUnit.SECONDS.sleep(3);

         cons2.close();

         MessageConsumer cons3 = session1.createConsumer(ActiveMQJMSClient.createQueue("myQueue.IAMEXPIRED"));

         int counter = 0;
         for(int i = 0 ; i <10 ; i ++) {

            TextMessage received = (TextMessage) cons3.receive(5000);
            server1.getPostOffice().getAddressManager().getBindings()
                    .filter(binding -> binding.getType() == BindingType.LOCAL_QUEUE)
                    .map(binding -> (Queue) binding.getBindable()).forEach((Queue q) -> {
                               System.err.println("\n\nQ: " + q.getName() + ", depth=" + q.getMessageCount());
                            });
            System.out.println(received);
            if(received != null) {
               counter++;
            }

         }

         System.out.println("Total messages received at expiry 1: " + counter);

      } finally {
         conn1.close();
         conn2.close();
      }
   }
}
