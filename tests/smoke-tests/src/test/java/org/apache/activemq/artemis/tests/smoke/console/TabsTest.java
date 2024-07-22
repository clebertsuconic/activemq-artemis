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
package org.apache.activemq.artemis.tests.smoke.console;

import static org.junit.jupiter.api.Assertions.fail;

import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.smoke.console.pages.LoginPage;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openqa.selenium.By;
import org.openqa.selenium.MutableCapabilities;
import org.openqa.selenium.NoSuchElementException;

//Parameters set in super class
@ExtendWith(ParameterizedTestExtension.class)
public class TabsTest extends ConsoleTest {

   public TabsTest(MutableCapabilities browserOptions) {
      super(browserOptions);
   }

   @TestTemplate
   public void testAllTabs() throws Throwable {
      testTab("connections", "Connections");
      testTab("sessions", "Sessions");
      testTab("consumers", "Consumers");
      testTab("producers", "Producers");
      testTab("addresses", "Addresses");
      testTab("queues", "Queues");
      testTabNegative("queues", "Connections");
      testTabNegative("connections", "Sessions");
      testTabNegative("connections", "Consumers");
      testTabNegative("connections", "roducers");
      testTabNegative("connections", "Addresses");
      testTabNegative("connections", "Queues");
   }

   private void testTab(String userpass, String tab) throws Throwable {
      driver.get(webServerUrl + "/console");
      new LoginPage(driver).loginValidUser(userpass, userpass, DEFAULT_TIMEOUT);
      driver.findElement(By.xpath("//a[contains(text(),'" + tab + "')]"));
      driver.close();
      startDriver();
   }

   private void testTabNegative(String userpass, String tab) throws Throwable {
      driver.get(webServerUrl + "/console");
      new LoginPage(driver).loginValidUser(userpass, userpass, DEFAULT_TIMEOUT);
      try {
         driver.findElement(By.xpath("//a[contains(text(),'" + tab + "')]"));
         fail("User " + userpass + " should not have been able to see the " + tab + " tab.");
      } catch (NoSuchElementException e) {
         // expected
      }
      driver.close();
      startDriver();
   }
}
