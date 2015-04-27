/**
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
package org.apache.activemq.artemis.tests.extras.byteman;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.protocol.stomp.StompProtocolManagerFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.tests.util.ServiceTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class StompInternalStateTest extends ServiceTestBase
{
   private static final String STOMP_QUEUE_NAME = "jms.queue.StompTestQueue";

   private static volatile String resultTestStompProtocolManagerLeak = null;

   protected ActiveMQServer server = null;

   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "StompProtocolManager Leak Server Rule",
                     targetClass = "org.apache.activemq.artemis.core.protocol.stomp.StompProtocolManager",
                     targetMethod = "onNotification(org.apache.activemq.artemis.core.server.management.Notification)",
                     targetLocation = "EXIT",
                     helper = "org.apache.activemq.tests.extras.byteman.StompInternalStateTest",
                     action = "verifyBindingAddRemove($1, $0.destinations)"
                  )
            }
      )
   public void testStompProtocolManagerLeak() throws Exception
   {
      ClientSession session = null;
      try
      {
         assertNull(resultTestStompProtocolManagerLeak);
         ServerLocator locator = createNettyNonHALocator();
         ClientSessionFactory factory = createSessionFactory(locator);
         session = factory.createSession();
         session.createTemporaryQueue(STOMP_QUEUE_NAME, STOMP_QUEUE_NAME);
         session.deleteQueue(STOMP_QUEUE_NAME);

         assertNull(resultTestStompProtocolManagerLeak);
      }
      finally
      {
         if (session != null)
         {
            session.close();
         }
      }
   }

   @Override
   protected Configuration createDefaultConfig(final boolean netty) throws Exception
   {
      Configuration config = super.createDefaultConfig(netty)
         .setSecurityEnabled(false)
         .setPersistenceEnabled(false);

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, StompProtocolManagerFactory.STOMP_PROTOCOL_NAME);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      params.put(TransportConstants.STOMP_CONSUMERS_CREDIT, "-1");
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);
      config.getAcceptorConfigurations().add(stompTransport);
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      return config;
   }

   @SuppressWarnings("unchecked")
   public void verifyBindingAddRemove(Notification noti, Object obj)
   {
      Set<String> destinations = (Set<String>)obj;
      if (noti.getType() == CoreNotificationType.BINDING_ADDED)
      {
         if (!destinations.contains(STOMP_QUEUE_NAME))
         {
            resultTestStompProtocolManagerLeak += "didn't save the queue when binding added " + destinations;
         }
      }
      else if (noti.getType() == CoreNotificationType.BINDING_REMOVED)
      {
         if (destinations.contains(STOMP_QUEUE_NAME))
         {
            resultTestStompProtocolManagerLeak = "didn't remove the queue when binding removed " + destinations;
         }
      }
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server = createServer(createDefaultConfig(true));
      server.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      server.stop();
      super.tearDown();
   }
}
