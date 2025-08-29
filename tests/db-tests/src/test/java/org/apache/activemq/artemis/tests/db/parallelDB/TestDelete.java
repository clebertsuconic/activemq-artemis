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

package org.apache.activemq.artemis.tests.db.parallelDB;

import javax.jms.*;

import org.apache.activemq.artemis.tests.util.CFUtil;
import org.junit.jupiter.api.Test;

public class TestDelete {



   @Test
   public void testSendAndCommit() throws Exception {

      ConnectionFactory cf = CFUtil.createConnectionFactory("OPENWIRE", "tcp://localhost:61616");
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createQueue("TEST"));
         for (int i = 0; i < 1500; i++) {
            producer.send(session.createTextMessage("hello"));
         }

         session.commit();



      }

   }

}
