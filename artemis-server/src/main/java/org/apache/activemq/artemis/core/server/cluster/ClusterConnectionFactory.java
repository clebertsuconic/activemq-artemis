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
package org.apache.activemq.artemis.core.server.cluster;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.impl.Validators;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.URISupport;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class ClusterConnectionFactory {
   public static final String TYPE_STATIC = "static";
   public static final String TYPE_DISCOVERY = "discovery";
   //the ccUri takes the following form:
   // static:(connector0,connector1)?cc-params
   // discovery:(discovery-name)?cc-params
   public static ClusterConnectionConfiguration createClusterConnection(URI ccUri) throws URISyntaxException {
      String scheme = ccUri.getScheme();
      if (TYPE_STATIC.equals(scheme)) {
         return createStaticClusterConnection(ccUri);
      }
      else if (TYPE_DISCOVERY.equals(scheme)) {
         return createDiscoveryClusterConnection(ccUri);
      }
      throw new IllegalArgumentException("Invalid cluster connection protocol: " + scheme);
   }

   private static ClusterConnectionConfiguration createStaticClusterConnection(URI ccUri) throws URISyntaxException {
      URISupport.CompositeData data = URISupport.parseComposite(ccUri);
      ClusterConnectionConfiguration config = parseCommonConfig(data);
      List<String> staticConnectorNames = new ArrayList<String>();
      URI[] staticUris = data.getComponents();
      for (URI uri : staticUris) {
         staticConnectorNames.add(uri.toString());
      }
      config.setStaticConnectors(staticConnectorNames);
      return config;
   }

   private static ClusterConnectionConfiguration parseCommonConfig(URISupport.CompositeData data) {
      Map<String, Object> params = data.getParameters();
      String ccName = getStringProperty("name", null, params, Validators.NOT_NULL_OR_EMPTY);
      String address = getStringProperty("address", null, params, Validators.NOT_NULL_OR_EMPTY);
      String connectorName = getStringProperty("connector-ref", null, params, Validators.NOT_NULL_OR_EMPTY);
      boolean dupDetection = ConfigurationHelper.getBooleanProperty("use-duplicate-detection",
              ActiveMQDefaultConfiguration.isDefaultClusterDuplicateDetection(), params);
      MessageLoadBalancingType messageLoadBalancingType;
      if (params.containsKey("forward-when-no-consumers")) {
         boolean forwardWhenNoConsumers = ConfigurationHelper.getBooleanProperty("forward-when-no-consumers",
                 ActiveMQDefaultConfiguration.isDefaultClusterForwardWhenNoConsumers(), params);
         if (forwardWhenNoConsumers) {
            messageLoadBalancingType = MessageLoadBalancingType.STRICT;
         }
         else {
            messageLoadBalancingType = MessageLoadBalancingType.ON_DEMAND;
         }
      }
      else {
         String messageLoadBalancing = getStringProperty("message-load-balancing",
                 ActiveMQDefaultConfiguration.getDefaultClusterMessageLoadBalancingType(),
                 params, Validators.MESSAGE_LOAD_BALANCING_TYPE);
         messageLoadBalancingType = Enum.valueOf(MessageLoadBalancingType.class, messageLoadBalancing);
      }

      int maxHops = getIntProperty("max-hops", ActiveMQDefaultConfiguration.getDefaultClusterMaxHops(), params, Validators.GE_ZERO);
      long clientFailureCheckPeriod = getLongProperty("check-period", ActiveMQDefaultConfiguration.getDefaultClusterFailureCheckPeriod(),
              params, Validators.GT_ZERO);
      long connectionTTL = getLongProperty("connection-ttl", ActiveMQDefaultConfiguration.getDefaultClusterConnectionTtl(), params, Validators.GT_ZERO);

      long retryInterval = getLongProperty("retry-interval", ActiveMQDefaultConfiguration.getDefaultClusterRetryInterval(), params, Validators.GT_ZERO);

      long callTimeout = getLongProperty("call-timeout", ActiveMQClient.DEFAULT_CALL_TIMEOUT, params, Validators.GT_ZERO);

      long callFailoverTimeout = getLongProperty("call-failover-timeout", ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT, params, Validators.MINUS_ONE_OR_GT_ZERO);

      double retryIntervalMultiplier = getDoubleProperty("retry-interval-multiplier", ActiveMQDefaultConfiguration.getDefaultClusterRetryIntervalMultiplier(), params, Validators.GT_ZERO);

      int minLargeMessageSize = getIntProperty("min-large-message-size", ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, params, Validators.GT_ZERO);

      long maxRetryInterval = getLongProperty("max-retry-interval", ActiveMQDefaultConfiguration.getDefaultClusterMaxRetryInterval(), params, Validators.GT_ZERO);

      int initialConnectAttempts = getIntProperty("initial-connect-attempts", ActiveMQDefaultConfiguration.getDefaultClusterInitialConnectAttempts(), params, Validators.MINUS_ONE_OR_GE_ZERO);

      int reconnectAttempts = getIntProperty("reconnect-attempts", ActiveMQDefaultConfiguration.getDefaultClusterReconnectAttempts(), params, Validators.MINUS_ONE_OR_GE_ZERO);

      int confirmationWindowSize = getIntProperty("confirmation-window-size", ActiveMQDefaultConfiguration.getDefaultClusterConfirmationWindowSize(), params, Validators.GT_ZERO);

      long clusterNotificationInterval = getLongProperty("notification-interval", ActiveMQDefaultConfiguration.getDefaultClusterNotificationInterval(), params, Validators.GT_ZERO);

      int clusterNotificationAttempts = getIntProperty("notification-attempts", ActiveMQDefaultConfiguration.getDefaultClusterNotificationAttempts(), params, Validators.GT_ZERO);

      boolean allowDirectConnectionsOnly = ConfigurationHelper.getBooleanProperty("allow-direct-connections-only", false, params);

      return new ClusterConnectionConfiguration().setName(ccName).setAddress(address).setConnectorName(connectorName).setMinLargeMessageSize(minLargeMessageSize).setClientFailureCheckPeriod(clientFailureCheckPeriod).setConnectionTTL(connectionTTL).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setMaxRetryInterval(maxRetryInterval).setInitialConnectAttempts(initialConnectAttempts).setReconnectAttempts(reconnectAttempts).setCallTimeout(callTimeout).setCallFailoverTimeout(callFailoverTimeout).setDuplicateDetection(dupDetection).setMessageLoadBalancingType(messageLoadBalancingType).setMaxHops(maxHops).setConfirmationWindowSize(confirmationWindowSize).setAllowDirectConnectionsOnly(allowDirectConnectionsOnly).setClusterNotificationInterval(clusterNotificationInterval).setClusterNotificationAttempts(clusterNotificationAttempts);
   }

   private static long getLongProperty(String name, long def, Map<String, Object> params, Validators.Validator validator) {
      long value = ConfigurationHelper.getLongProperty(name, def, params);
      validator.validate(name, value);
      return value;
   }

   private static double getDoubleProperty(String name, double def, Map<String, Object> params, Validators.Validator validator) {
      double value = ConfigurationHelper.getDoubleProperty(name, def, params);
      validator.validate(name, value);
      return value;
   }

   private static int getIntProperty(String name, int def, Map<String, Object> params, Validators.Validator validator) {
      int value = ConfigurationHelper.getIntProperty(name, def, params);
      validator.validate(name, value);
      return value;
   }

   private static String getStringProperty(String name, String def, Map<String, Object> params, Validators.Validator validator) {
      String value = ConfigurationHelper.getStringProperty(name, def, params);
      validator.validate(name, value);
      return value;
   }

   private static ClusterConnectionConfiguration createDiscoveryClusterConnection(URI ccUri) throws URISyntaxException {
      URISupport.CompositeData data = URISupport.parseComposite(ccUri);
      ClusterConnectionConfiguration config = parseCommonConfig(data);
      URI discoveryName = data.getComponents()[0];
      config.setDiscoveryGroupName(discoveryName.toString());
      return config;
   }

   //move this to test
   public static void main(String[] args) throws Exception {
      //URI sampleUri = new URI("static://(tcp://localhost:5455?something=true,tcp://localhost:16161?something=false)?name=cluster&address=jms");
      URI sampleUri = new URI("static:(name1,name2)?name=cluster&address=jms&connector-ref=netty");
      //URI sampleUri = new URI("jgroups:(?channel-name=channel1)?name=cluster&address=jms");
      URISupport.CompositeData data = URISupport.parseComposite(sampleUri);
      System.out.println("path: " + data.getPath());
      URI[] components = data.getComponents();
      for (URI c : components) {
         System.out.println("comp: " + c);
         System.out.println("comp prop: " + c.getQuery());
      }
      System.out.println("fragment: " + data.getFragment());
      System.out.println("host: " + data.getHost());

      System.out.println("params: " + data.getParameters());
      System.out.println("scheme: " + data.getScheme());
   }
}
