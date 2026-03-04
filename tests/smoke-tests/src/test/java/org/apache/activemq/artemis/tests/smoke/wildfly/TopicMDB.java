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

package org.apache.activemq.artemis.tests.smoke.wildfly;

import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MDB that consumes from SecondaryTopic
 */
@MessageDriven(
   name = "TopicMDB",
   activationConfig = {
      @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Topic"),
      @ActivationConfigProperty(propertyName = "destination", propertyValue = "java:jboss/exported/jms/topic/SecondaryTopic"),
      @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge")
   }
)
public class TopicMDB implements MessageListener {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final AtomicInteger messageCount = new AtomicInteger(0);

   @Override
   public void onMessage(Message message) {
      try {
         if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            String text = textMessage.getText();
            int count = messageCount.incrementAndGet();
            logger.info("TopicMDB received message #{}: {}", count, text);
         }
      } catch (JMSException e) {
         logger.error("Error processing message in TopicMDB", e);
         throw new RuntimeException(e);
      }
   }

   public static int getMessageCount() {
      return messageCount.get();
   }

   public static void resetMessageCount() {
      messageCount.set(0);
   }
}
