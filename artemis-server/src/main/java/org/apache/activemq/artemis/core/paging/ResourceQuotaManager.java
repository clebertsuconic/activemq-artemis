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
package org.apache.activemq.artemis.core.paging;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuota;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Manages resource quotas including template instantiation and parent hierarchy resolution.
 * <p>
 * This manager handles:
 * <ul>
 *   <li>Storage and retrieval of quota definitions</li>
 *   <li>Wildcard template expansion (e.g., "EU.*" template creates "EU.fr" instance)</li>
 *   <li>Parent-child quota hierarchy establishment</li>
 *   <li>Thread-safe quota instance creation</li>
 * </ul>
 */
public class ResourceQuotaManager {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final HierarchicalObjectRepository<ResourceQuota> quotaRepository;
   private final ConcurrentHashMap<String, ResourceQuota> instantiatedQuotas;
   private final WildcardConfiguration wildcardConfiguration;

   public ResourceQuotaManager(HierarchicalObjectRepository<ResourceQuota> quotaRepository,
                               WildcardConfiguration wildcardConfiguration) {
      this.quotaRepository = quotaRepository;
      this.instantiatedQuotas = new ConcurrentHashMap<>();
      this.wildcardConfiguration = wildcardConfiguration;
   }

   /**
    * Get the resource quota for a given address based on its settings.
    *
    * @param address  the address to get quota for
    * @param settings the address settings containing quota reference
    * @return the resource quota, or null if none configured
    */
   public ResourceQuota getQuotaForAddress(SimpleString address, AddressSettings settings) {
      if (settings == null || settings.getResourceQuota() == null) {
         return null;
      }

      String quotaName = settings.getResourceQuota();

      // Check if quota name contains wildcard - if so, need to resolve instance
      if (quotaName.contains("*")) {
         return resolveWildcardQuota(quotaName, address);
      }

      // Simple case: direct quota lookup
      ResourceQuota quota = quotaRepository.getMatch(quotaName);
      if (quota == null) {
         logger.warn("Quota {} referenced but not found for address {}", quotaName, address);
      }
      return quota;
   }

   /**
    * Resolve a wildcard quota template to a concrete instance.
    * For example, quota "EU.*" with address "eu.fr.orders" becomes instance "EU.fr"
    *
    * @param quotaTemplate the quota template name (e.g., "EU.*")
    * @param address       the address to match
    * @return resolved quota instance, or null if template not found
    */
   private ResourceQuota resolveWildcardQuota(String quotaTemplate, SimpleString address) {
      // First check if template exists
      ResourceQuota template = quotaRepository.getMatch(quotaTemplate);
      if (template == null) {
         logger.warn("Quota template {} not found", quotaTemplate);
         return null;
      }

      // Extract wildcard value from address
      // For quota "EU.*" and address "eu.fr.orders", extract "fr"
      String wildcardValue = extractWildcardValue(address.toString(), quotaTemplate);
      if (wildcardValue == null) {
         logger.debug("Could not extract wildcard value for quota {} from address {}", quotaTemplate, address);
         return template; // Fall back to template itself
      }

      // Build instance name by substituting wildcard
      String instanceName = quotaTemplate.replace("*", wildcardValue);

      // Get or create the instance
      return instantiatedQuotas.computeIfAbsent(instanceName, name -> {
         logger.info("Creating quota instance {} from template {}", name, quotaTemplate);
         return createQuotaInstance(name, template);
      });
   }

   /**
    * Extract the wildcard value from an address.
    * For quota "EU.*" we expect addresses like "eu.XX.*" where XX is the wildcard value.
    *
    * @param addressStr    the address string
    * @param quotaTemplate the quota template (e.g., "EU.*")
    * @return the extracted wildcard value, or null if not found
    */
   private String extractWildcardValue(String addressStr, String quotaTemplate) {
      // Convert quota template to lowercase prefix for matching
      // "EU.*" becomes "eu."
      String templatePrefix = quotaTemplate.substring(0, quotaTemplate.indexOf('*')).toLowerCase();

      // Check if address starts with this prefix pattern
      String[] addressParts = addressStr.split("\\.");
      String[] templateParts = templatePrefix.split("\\.");

      // Find the position of the wildcard in the template
      int wildcardIndex = templateParts.length;

      // Extract the value at that position from the address
      if (addressParts.length > wildcardIndex) {
         return addressParts[wildcardIndex];
      }

      return null;
   }

   /**
    * Create a new quota instance from a template.
    *
    * @param instanceName the name for the new instance (e.g., "EU.fr")
    * @param template     the template to copy from
    * @return the new quota instance
    */
   private ResourceQuota createQuotaInstance(String instanceName, ResourceQuota template) {
      ResourceQuota instance = template.copy(instanceName);

      // Establish parent relationship if template has one
      if (instance.getPartOf() != null) {
         ResourceQuota parent = quotaRepository.getMatch(instance.getPartOf());
         if (parent != null) {
            instance.setParent(parent);
         } else {
            logger.warn("Parent quota {} not found for instance {}", instance.getPartOf(), instanceName);
         }
      }

      return instance;
   }

   /**
    * Establish parent-child relationships for all quotas in the repository.
    * This should be called after all quotas are loaded from configuration.
    */
   public void establishParentRelationships(Map<String, ResourceQuota> allQuotas) {
      Set<String> visited = new HashSet<>();

      for (ResourceQuota quota : allQuotas.values()) {
         establishParentChain(quota, allQuotas, visited);
      }

      logger.info("Established parent relationships for {} quotas", allQuotas.size());
   }

   /**
    * Recursively establish parent chain for a quota, detecting circular references.
    *
    * @param quota      the quota to process
    * @param allQuotas  all available quotas
    * @param visited    set of quota names already visited (for cycle detection)
    */
   private void establishParentChain(ResourceQuota quota, Map<String, ResourceQuota> allQuotas, Set<String> visited) {
      if (quota == null || quota.getPartOf() == null) {
         return;
      }

      // Already processed
      if (quota.getParent() != null) {
         return;
      }

      // Detect circular reference
      if (visited.contains(quota.getName())) {
         logger.error("Circular parent reference detected for quota: {}", quota.getName());
         return;
      }

      visited.add(quota.getName());

      String parentName = quota.getPartOf();
      ResourceQuota parent = allQuotas.get(parentName);

      if (parent == null) {
         logger.warn("Parent quota {} not found for quota {}", parentName, quota.getName());
         return;
      }

      // Recursively establish parent's chain first
      establishParentChain(parent, allQuotas, visited);

      // Now set the parent
      quota.setParent(parent);
      logger.debug("Established parent relationship: {} -> {}", quota.getName(), parent.getName());

      visited.remove(quota.getName());
   }

   /**
    * Add a quota to the repository.
    *
    * @param name  the quota name
    * @param quota the quota object
    */
   public void addQuota(String name, ResourceQuota quota) {
      quotaRepository.addMatch(name, quota);
      logger.debug("Added quota: {}", name);
   }

   /**
    * Get a quota by exact name.
    *
    * @param name the quota name
    * @return the quota, or null if not found
    */
   public ResourceQuota getQuota(String name) {
      return quotaRepository.getMatch(name);
   }

   /**
    * Get all configured quotas (not including runtime-created instances).
    *
    * @return map of quota name to quota object
    */
   public Map<String, ResourceQuota> getAllQuotas() {
      // Note: HierarchicalObjectRepository doesn't expose direct access to all matches
      // This will need to be enhanced when we need management API
      return new ConcurrentHashMap<>(instantiatedQuotas);
   }

   /**
    * Get all instantiated quotas created from templates.
    *
    * @return map of instance name to quota object
    */
   public Map<String, ResourceQuota> getInstantiatedQuotas() {
      return new ConcurrentHashMap<>(instantiatedQuotas);
   }

   /**
    * Clear all runtime-created quota instances.
    * Called on broker shutdown or configuration reload.
    */
   public void clearInstances() {
      instantiatedQuotas.clear();
      logger.info("Cleared all instantiated quotas");
   }

   @Override
   public String toString() {
      return "ResourceQuotaManager{" +
             "instantiatedQuotas=" + instantiatedQuotas.size() +
             '}';
   }
}
