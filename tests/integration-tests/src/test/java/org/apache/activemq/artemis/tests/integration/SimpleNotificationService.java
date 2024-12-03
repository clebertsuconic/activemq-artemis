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
package org.apache.activemq.artemis.tests.integration;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.management.NotificationType;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.core.server.management.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleNotificationService implements NotificationService {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final List<NotificationListener> listeners = new ArrayList<>();


   // NotificationService implementation ----------------------------

   @Override
   public void addNotificationListener(final NotificationListener listener) {
      listeners.add(listener);
   }

   @Override
   public void enableNotifications(final boolean enable) {
   }

   @Override
   public void removeNotificationListener(final NotificationListener listener) {
      listeners.remove(listener);
   }

   @Override
   public void sendNotification(final Notification notification) throws Exception {
      for (NotificationListener listener : listeners) {
         listener.onNotification(notification);
      }
   }


   public static class Listener implements NotificationListener {

      public int count(NotificationType... interestingTypes) {
         if (logger.isDebugEnabled()) {
            logger.debug("count for {}", stringOf(interestingTypes));
         }
         return (int) notifications.stream().filter(n -> matchTypes(n, interestingTypes)).count();
      }

      private static String stringOf(NotificationType[] types) {
         StringBuilder builder = new StringBuilder();
         builder.append("types[" + types.length + "] = {");
         for (int i = 0; i < types.length; i++) {
            builder.append(types[i]);
            if (i + 1 < types.length) {
               builder.append(",");
            }
         }
         builder.append("}");
         return builder.toString();
      }

      public int size() {
         return notifications.size();
      }

      private boolean matchTypes(Notification notification, NotificationType... interestingTypes) {
         logger.debug("matching {}", notification);
         for (NotificationType t : interestingTypes) {
            logger.debug("looking to match {} with type parameter {}", notification, t);
            if (notification.getType() == t) {
               return true;
            }
         }
         return false;
      }

      public Notification findAny(NotificationType notificationType) {
         return notifications.stream().filter(n -> n.getType() == notificationType).findAny().get();
      }

      ////////////////////////////////////////////////////////////////////////////////////
      // Note: Please do not expose this collection
      // Instead,  make sure that you find the notifications you want by the types you are interested.
      // some tests in the past validated if this collection was empty when at a later point new notifications were added
      // and the test became flaky as it was receiving a notification it was not interested with
      private final List<Notification> notifications = new ArrayList<>();

      @Override
      public void onNotification(final Notification notification) {
         notifications.add(notification);
      }

   }
}
