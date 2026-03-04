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
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.logging.Logger;

@MessageDriven(activationConfig = {
   @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
   @ActivationConfigProperty(propertyName = "destination", propertyValue = "InitialQueue"),
   @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge")
})
public class FirstMDB implements MessageListener {
   private static final Logger logger = Logger.getLogger(FirstMDB.class.getName());

   @Override
   public void onMessage(Message message) {
      try {
         TextMessage textMessage = (TextMessage) message;
         String text = textMessage.getText();
         logger.info("FirstMDB received: " + text);

         // Message is automatically acknowledged
         message.acknowledge();
      } catch (Exception e) {
         logger.severe("Error processing message: " + e.getMessage());
         e.printStackTrace();
      }
   }
}
