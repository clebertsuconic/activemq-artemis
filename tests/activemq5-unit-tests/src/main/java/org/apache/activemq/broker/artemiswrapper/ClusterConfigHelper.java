package org.apache.activemq.broker.artemiswrapper;

import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.URISupport;

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

/**
 * A Global class that assists cluster forming.
 */
public class ClusterConfigHelper {

   private static Map<String, Map<String, NetworkConnector>> duplexMap = new HashMap<String, Map<String, NetworkConnector>>();

   public static void registerDuplex(String local, NetworkConnector nc) {
      Map<String, NetworkConnector> remoteSet = duplexMap.get(local);
      if (remoteSet == null) {
         remoteSet = new HashMap<String, NetworkConnector>();
         duplexMap.put(local, remoteSet);
      }
      remoteSet.put(nc.getBrokerURL(), nc);
   }

   public static List<NetworkConnector> getDuplexConnections(String remote) {
      List<NetworkConnector> duplexList = new ArrayList<NetworkConnector>();
      Iterator<Map<String, NetworkConnector>> iterator = duplexMap.values().iterator();
      while (iterator.hasNext()) {
         Map<String, NetworkConnector> remoteSet = iterator.next();
         NetworkConnector remoteNC = remoteSet.get(remote);
         if (remoteNC != null) {
            duplexList.add(remoteNC);
         }
      }
      return duplexList;
   }

   //because the discoveryUri is private we use reflection to get it
   public static URI getDiscoveryUri(DiscoveryNetworkConnector dnc) throws NoSuchFieldException, IllegalAccessException {
      Field field = DiscoveryNetworkConnector.class.getDeclaredField("discoveryUri");
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

}
