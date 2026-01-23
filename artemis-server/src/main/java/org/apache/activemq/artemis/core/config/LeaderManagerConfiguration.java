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

package org.apache.activemq.artemis.core.config;

import java.util.HashMap;
import java.util.Map;

public class LeaderManagerConfiguration {

   String name;
   String lockId;
   String type;
   int checkPeriod;
   Map<String, String> properties;

   public LeaderManagerConfiguration() {
      properties = new HashMap<>();
   }

   public LeaderManagerConfiguration(Map<String, String> properties) {
      this.properties = properties;
   }

   public String getName() {
      return name;
   }

   public LeaderManagerConfiguration setName(String name) {
      if (name == null || name.trim().isEmpty()) {
         throw new IllegalArgumentException("LeadManager name cannot be null or empty");
      }
      this.name = name;
      return this;
   }

   public String getLockId() {
      return lockId;
   }

   public LeaderManagerConfiguration setLockId(String lockId) {
      if (lockId == null || lockId.trim().isEmpty()) {
         throw new IllegalArgumentException("LeadManager lockId cannot be null or empty");
      }
      this.lockId = lockId;
      return this;
   }

   public String getLeadType() {
      return type;
   }

   public LeaderManagerConfiguration setLeadType(String type) {
      if (type == null || type.trim().isEmpty()) {
         throw new IllegalArgumentException("LeadManager type cannot be null or empty");
      }
      this.type = type;
      return this;
   }

   public int getCheckPeriod() {
      return checkPeriod;
   }

   public LeaderManagerConfiguration setCheckPeriod(int checkPeriod) {
      if (checkPeriod <= 0) {
         throw new IllegalArgumentException("LeadManager checkPeriod must be positive, got: " + checkPeriod);
      }
      this.checkPeriod = checkPeriod;
      return this;
   }

   public Map<String, String> getProperties() {
      return properties;
   }

   @Override
   public String toString() {
      return "LeadManagerConfiguration{" + "name='" + name + '\'' + ", lockId='" + lockId + '\'' + ", type='" + type + '\'' + ", checkPeriod=" + checkPeriod + ", properties=" + properties + '}';
   }
}
