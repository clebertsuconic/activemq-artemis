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

package org.apache.activemq.artemis.lockmanager.etcd;

public class EtcdLockConfiguration {

   public EtcdLockConfiguration() {
   }

   private String[] endpoints;
   private String user;
   private String password;
   private String authority;
   private int leasePeriod;

   public int getLeasePeriod() {
      return leasePeriod;
   }

   public EtcdLockConfiguration setLeasePeriod(int leasePeriod) {
      this.leasePeriod = leasePeriod;
      return this;
   }

   public String[] getEndpointArray() {
      return endpoints;
   }

   public String getEndpoints() {
      return String.join(",", endpoints);
   }

   public EtcdLockConfiguration setEndpoints(String endpoints) {
      this.endpoints = endpoints.split(",");
      return this;
   }

   public String getUser() {
      return user;
   }

   public EtcdLockConfiguration setUser(String user) {
      this.user = user;
      return this;
   }

   public String getPassword() {
      return password;
   }

   public EtcdLockConfiguration setPassword(String password) {
      this.password = password;
      return this;
   }

   public String getAuthority() {
      return authority;
   }

   public EtcdLockConfiguration setAuthority(String authority) {
      this.authority = authority;
      return this;
   }
}
