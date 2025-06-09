/*
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
package org.apache.activemq.artemis.utils.actors;

import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PauseActorTest {

   Actor<Integer> actor;

   HashSet<Integer> receivedValues = new HashSet<>();

   public void doInteger(Integer received) {
      receivedValues.add(received);
      if (received.equals(Integer.valueOf(9))) {
         actor.pauseProcessing();
      }
   }

   @Test
   public void testPauseActor() throws Exception {
      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      try {
         actor = new Actor<>(executorService, this::doInteger);

         for (int i = 0; i < 20; i++) {
            actor.act(i);
         }

         actor.flush();
         Wait.assertEquals(10, () -> receivedValues.size());

         for (int i = 20; i < 30; i++) {
            actor.act(i);
         }

         for (int i = 0; i < 30; i++) {
            if (i < 10) {
               assertTrue(receivedValues.contains(i));
            } else {
               assertFalse(receivedValues.contains(i));
            }
         }

         actor.resumeProcessing();
         actor.flush();
         Wait.assertEquals(30, () -> receivedValues.size());

         for (int i = 0; i < 30; i++) {
            assertTrue(receivedValues.contains(i));
         }
      } finally {
         executorService.shutdown();
      }
   }

 }