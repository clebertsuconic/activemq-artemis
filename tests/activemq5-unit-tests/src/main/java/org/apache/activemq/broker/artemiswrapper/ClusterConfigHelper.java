/**
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
package org.apache.activemq.broker.artemiswrapper;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.postoffice.impl.AddressImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.URISupport;

/**
 * A Global class that assists cluster forming.
 */
public class ClusterConfigHelper {

   private static Map<String, List<NCRemoteSet>> duplexMap = new HashMap<String, List<NCRemoteSet>>();

   public static void registerDuplex(String local, DiscoveryNetworkConnector nc) throws NoSuchFieldException, IllegalAccessException, URISyntaxException {
      List<NCRemoteSet> ncList = duplexMap.get(local);
      if (ncList == null) {
         ncList = new ArrayList<NCRemoteSet>();
         duplexMap.put(local, ncList);
      }
      URI discoveryUri = ClusterConfigHelper.getDiscoveryUri(nc);
      URI[] remoteList = ClusterConfigHelper.extractRemoteUris(discoveryUri);
      NCRemoteSet ncSet = new NCRemoteSet(nc);
      for (URI remoteUri : remoteList) {
         ncSet.addRemote(remoteUri);
      }
      ncList.add(ncSet);
   }

   public static List<NCPair> getDuplexConnections(String remote) throws URISyntaxException {
      List<NCPair> duplexList = new ArrayList<NCPair>();
      Iterator<String> localUrlIterator = duplexMap.keySet().iterator();
      while (localUrlIterator.hasNext()) {
         String localUrl = localUrlIterator.next();
         List<NCRemoteSet> listSet = duplexMap.get(localUrl);
         for (NCRemoteSet ncSet : listSet) {
            if (ncSet.contains(remote)) {
               duplexList.add(new NCPair(localUrl, ncSet.nc));
            }
         }
      }
      return duplexList;
   }

   //because the discoveryUri is private we use reflection to get it
   public static URI getDiscoveryUri(DiscoveryNetworkConnector dnc) throws NoSuchFieldException, IllegalAccessException {
      Field field = DiscoveryNetworkConnector.class.getDeclaredField("discoveryUri");
      field.setAccessible(true);
      return (URI)field.get(dnc);
   }

   public static URI[] extractRemoteUris(URI discoveryUri) throws URISyntaxException {
      String scheme = discoveryUri.getScheme();
      if ("static".equals(scheme)) {
         URISupport.CompositeData data = URISupport.parseComposite(discoveryUri);
         System.out.println("data params: " + data.getParameters());
         URI[] comps = data.getComponents();
         for (int i = 0; i < comps.length; i++) {
            System.out.println("data componet: " + comps[i].toString());
         }
         return comps;
      }
      else {
         throw new IllegalStateException("Please implement to support this scheme: " + scheme);
      }
   }

   public static void main(String[] args) throws Exception {
      URI discoveryUri = new URI("static:(tcp://host1:61616,tcp://host2:61616)");
      ClusterConfigHelper.extractRemoteUris(discoveryUri);
      ClusterConnectionConfiguration ccCfg = new ClusterConnectionConfiguration();
      System.out.println("default CC address: " + ccCfg.getAddress());
   }

   public static TransportConfiguration getCCConnector(NetworkConnector nc) throws URISyntaxException {
      String connectorName = "from" + nc.getName();
      URI localUri = nc.getLocalUri();
      return createConnectorFromUri(localUri, connectorName);
   }

   //to do: consider messageTTL and consumerTTL
   public static int getMaxHops(NetworkConnector nc) {
      return nc.getNetworkTTL();
   }

   public static TransportConfiguration createCCStaticConnector(URI remote, int i) {
      return createConnectorFromUri(remote, "to" + i);
   }

   public static TransportConfiguration createConnectorFromUri(URI uri, String name) {
      String factoryClass = "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory";
      Map<String, Object> params = new HashMap<String, Object>();
      String host = uri.getHost();
      int port = uri.getPort();
      params.put(TransportConstants.HOST_PROP_NAME, host);
      params.put(TransportConstants.PORT_PROP_NAME, port);
      TransportConfiguration connectorConfig = new TransportConfiguration(factoryClass, params, name);

      return connectorConfig;
   }

   /*
    * NetworkConnector has three related parameters:
    * ExcludedDestinations
    * DynamicallyIncludedDestinations
    * StaticallyIncludedDestinations
    * we need to anaylze them and build
    * equivalent addresses.
    */
   public static AddressImpl[] getEquivalentAddresses(DiscoveryNetworkConnector nc) {

      AddressImpl defaultAddress = new AddressImpl(new SimpleString("jms"));

      List<ActiveMQDestination> excludes = nc.getExcludedDestinations();
      List<AddressImpl> excludeSet = new ArrayList<AddressImpl>();
      for (ActiveMQDestination dest : excludes) {
         //replace ".>" wildcard
         String rawDestName = dest.getPhysicalName().replace(".>", ".#");

         excludeSet.add(new AddressImpl(new SimpleString(rawDestName)));
      }
      List<ActiveMQDestination> dynamicIncludes =nc.getDynamicallyIncludedDestinations();
      List<AddressImpl> includeSet = new ArrayList<AddressImpl>();
      for (ActiveMQDestination dest : dynamicIncludes) {
         //replace ".>" wildcard
         String rawDestName = dest.getPhysicalName().replace(".>", ".#");

         includeSet.add(new AddressImpl(new SimpleString(rawDestName)));
      }
      List<ActiveMQDestination> staticIncludes = nc.getStaticallyIncludedDestinations();
      for (ActiveMQDestination dest : staticIncludes) {
         //".>" is not allowed
         String rawDestName = dest.getPhysicalName();

         includeSet.add(new AddressImpl(new SimpleString(rawDestName)));
      }

      //check exclude
      for (AddressImpl incAddr : includeSet) {
         if (incAddr.containsWildCard()) {
            throw new IllegalStateException("Cluster Connection doesn't support wildcard address: " +
            incAddr.getAddress() + " Please reconfigure your test.");
         }
         for (AddressImpl excAddr : excludeSet) {
            if (incAddr.matches(excAddr)) {
               throw new IllegalStateException("Currently we don't support excludes in NetworkConnector \n" +
               "and there is a exclude address: " + excAddr.getAddress() + " that conflicts with one of the \n" +
               "includes: " + incAddr.getAddress() + ". Please reconfigure the test.");
            }
         }
      }
      if (includeSet.size() == 0) {
         includeSet.add(defaultAddress);
      }

      return includeSet.toArray(new AddressImpl[0]);
   }

   private static class NCRemoteSet
   {
      public DiscoveryNetworkConnector nc;
      public Set<URI> remoteSet = new HashSet<URI>();

      public NCRemoteSet(DiscoveryNetworkConnector nc) {
         this.nc = nc;
      }

      public void addRemote(URI remoteUri) {
         this.remoteSet.add(remoteUri);
      }

      public boolean contains(String remote) throws URISyntaxException {
         URI remoteUri = new URI(remote);
         String remoteHost = remoteUri.getHost();
         int remotePort = remoteUri.getPort();
         Iterator<URI> iter = remoteSet.iterator();
         while (iter.hasNext()) {
            URI uri = iter.next();
            String host = uri.getHost();
            int port = uri.getPort();
            if (remoteHost.equals(host) && port == remotePort) {
               return true;
            }
         }
         return false;
      }
   }

   public static class NCPair
   {
      public String localUri;
      public DiscoveryNetworkConnector nc;

      public NCPair(String localUrl, DiscoveryNetworkConnector nc) {
         this.localUri = localUrl;
         this.nc = nc;
      }
   }
}
