# Resource Quotas

## Overview

Resource quotas provide hierarchical limits on broker resources to prevent any single tenant, region, or application from consuming excessive broker capacity. Unlike global limits which apply to the entire broker, resource quotas allow fine-grained control over resource consumption across organizational boundaries.

Resource quotas can limit three types of resources:
- **Message bytes**: Total memory consumed by messages across all addresses in the quota
- **Address count**: Maximum number of addresses that can be created within the quota
- **Queue count**: Maximum number of queues that can be created within the quota

## Key Features

### Hierarchical Quotas

Quotas can be organized in parent-child hierarchies where child quota usage counts toward parent limits. This allows modeling organizational structures like:
- Enterprise → Region → Tenant
- Department → Team → Application
- Global → Geography → Customer

Child quotas are constrained by both their own limits AND their parent's limits, enforcing quotas at multiple organizational levels simultaneously.

### Wildcard Templates

Quota templates with wildcards automatically create quota instances when addresses match the pattern. For example:
- Template `EU.*` with address `eu.fr.orders` creates instance `EU.fr`
- Template `tenant.*` with address `tenant.acme.data` creates instance `tenant.acme`

Instances inherit the template's limits and parent relationships, allowing dynamic multi-tenant configurations.

### Integration with Address Settings

Quotas are referenced from address-settings patterns, allowing different address namespaces to participate in different quota schemes. An address can only belong to one quota, determined by its matching address-settings entry.

## Configuration

### Defining Resource Quotas

Resource quotas are defined in the `<resource-quotas>` section of `broker.xml`:

```xml
<configuration>
   <core>
      <resource-quotas>
         <!-- Top-level quota for Europe -->
         <resource-quota name="EUROPE">
            <max-message-bytes>10G</max-message-bytes>
            <max-addresses>1000</max-addresses>
            <max-queues>5000</max-queues>
         </resource-quota>

         <!-- Country-level template - creates instances like EU.fr, EU.de -->
         <resource-quota name="EU.*">
            <max-message-bytes>2G</max-message-bytes>
            <max-addresses>200</max-addresses>
            <max-queues>1000</max-queues>
            <part-of>EUROPE</part-of>
         </resource-quota>

         <!-- Tenant-level quota within a country -->
         <resource-quota name="tenant.acme">
            <max-message-bytes>500M</max-message-bytes>
            <max-addresses>50</max-addresses>
            <max-queues>100</max-queues>
            <part-of>EU.fr</part-of>
         </resource-quota>
      </resource-quotas>
   </core>
</configuration>
```

### Configuration Elements

#### `<resource-quota name="...">`
Defines a quota with a unique name. The name can contain wildcards (`*`) for template instantiation.

**Attributes:**
- `name` (required): Unique identifier for the quota

**Child Elements:**
- `<max-message-bytes>`: Maximum total bytes for messages (supports K, M, G suffixes). Default: unlimited (-1)
- `<max-addresses>`: Maximum number of addresses. Default: unlimited (-1)  
- `<max-queues>`: Maximum number of queues. Default: unlimited (-1)
- `<part-of>`: Parent quota name for hierarchical enforcement. Optional.

### Assigning Quotas to Addresses

Reference quotas from address-settings using the `<resource-quota>` element:

```xml
<address-settings>
   <!-- European tenant addresses use EU wildcard template -->
   <address-setting match="eu.#">
      <resource-quota>EU.*</resource-quota>
   </address-setting>

   <!-- Specific tenant quota -->
   <address-setting match="tenant.acme.#">
      <resource-quota>tenant.acme</resource-quota>
   </address-setting>

   <!-- US addresses use different quota -->
   <address-setting match="us.#">
      <resource-quota>US.*</resource-quota>
   </address-setting>
</address-settings>
```

## How It Works

### Quota Acquisition and Enforcement

Resource quotas enforce limits on three resource types:

1. **Address creation**: Acquires an `AddressQuotaToken` that increments the address count. If the limit would be exceeded (checking child and all parents in the hierarchy), the token acquisition throws an exception and the address is not created.

2. **Queue creation**: Acquires a `QueueQuotaToken` that increments the queue count. Same hierarchical enforcement as addresses.

3. **Message bytes**: Tracked directly as messages are added to the paging store. When a message is routed, the broker checks if the byte quota would be exceeded (checking child and all parents). If so, the message is rejected with an exception.

Tokens use a try-with-resources pattern with automatic rollback:

```java
try (QuotaToken token = quotaService.acquireAddressToken(address, false)) {
   // Create address...
   token.commit(); // Success - keep the quota increment
} // Auto-rollback on exception or if commit() not called
```

Message bytes are tracked differently - they are updated directly in the paging store without using tokens, and limits are enforced when messages are routed.

### Hierarchical Enforcement

When checking limits, quotas walk up the parent chain:

1. Check if child quota limit would be exceeded
2. Check if parent quota limit would be exceeded (recursively up the chain)
3. If any ancestor would be exceeded, reject the operation
4. Otherwise, increment counters in child and all ancestors

This ensures that parent limits are respected even when child limits are generous.

### Wildcard Template Resolution

When an address is created with a wildcard quota reference:

1. Extract the wildcard value from the address name (e.g., `eu.fr.orders` → `fr`)
2. Create instance name by substituting wildcard (e.g., `EU.*` → `EU.fr`)
3. Check if instance already exists; if not, create from template
4. Establish parent relationship if template has `<part-of>`
5. Return the quota instance for enforcement

Instances are created lazily and cached for the broker's lifetime.

## Behavior on Limit Exceeded

When a quota limit is exceeded, the broker throws `ActiveMQResourceQuotaExceededException`:

- **Address creation**: Address creation fails with quota exception
- **Queue creation**: Queue creation fails with quota exception
- **Message routing**: Message is rejected at send time (before entering the broker)

Clients receive the exception and can handle it (retry, route elsewhere, alert, etc).

## Counter Rebuild on Restart

Resource quota counters are **not persisted**. After broker restart:

1. Quota configurations are loaded from `broker.xml`
2. Quota instances are created with zero counters
3. During journal replay, the broker scans existing addresses and queues
4. Counters are rebuilt by incrementing for each discovered resource
5. Parent relationships are re-established
6. Broker continues with accurate counts

This approach avoids counter persistence complexity and ensures counters stay accurate despite any crashes.

## Monitoring and Management

### JMX Metrics

Resource quotas expose metrics via JMX (implementation pending):
- Current message bytes / max bytes
- Current address count / max addresses  
- Current queue count / max queues
- Utilization percentage for each dimension
- Parent quota reference

### Logging

Quota operations are logged at DEBUG level:
- Quota instance creation from templates
- Counter increments/decrements  
- Limit checks and rejections
- Parent relationship establishment

Enable DEBUG logging for `org.apache.activemq.artemis.core.paging.ResourceQuotaManager` and `org.apache.activemq.artemis.core.settings.impl.ResourceQuota` to trace quota activity.

## Best Practices

### Quota Hierarchy Design

1. **Top-down planning**: Design your quota hierarchy to match your organizational structure
2. **Conservative parent limits**: Set parent limits lower than the sum of child limits to leave headroom
3. **Monitor utilization**: Track quota usage over time to adjust limits appropriately
4. **Avoid deep hierarchies**: Keep hierarchies to 3-4 levels maximum for performance

### Wildcard Template Usage

1. **Consistent naming**: Use predictable address naming conventions for wildcard extraction to work
2. **Document patterns**: Clearly document which wildcard templates exist and what they match
3. **Template testing**: Test that addresses correctly create expected instances
4. **Instance cleanup**: Consider quota instance lifecycle (currently kept for broker lifetime)

### Address Settings Integration

1. **Non-overlapping patterns**: Ensure address-setting patterns don't assign multiple quotas to the same address
2. **Explicit is better**: Use specific quota names over wildcards when the set of quotas is known upfront
3. **Default settings**: Consider a catch-all address-setting without quota for unquoted addresses

### Limit Configuration

1. **Start conservative**: Begin with stricter limits and relax based on monitoring
2. **Balanced limits**: Set limits proportionally across dimensions (bytes, addresses, queues)
3. **Test limits**: Verify quota exceptions are handled gracefully by clients
4. **Leave headroom**: Account for burst traffic and growth when setting limits

## Examples

### Multi-Tenant SaaS Platform

```xml
<resource-quotas>
   <!-- Root quota for all tenants -->
   <resource-quota name="TENANTS">
      <max-message-bytes>50G</max-message-bytes>
      <max-addresses>10000</max-addresses>
      <max-queues>50000</max-queues>
   </resource-quota>

   <!-- Tenant template - creates tenant.acme, tenant.beta, etc -->
   <resource-quota name="tenant.*">
      <max-message-bytes>5G</max-message-bytes>
      <max-addresses>1000</max-addresses>
      <max-queues>5000</max-queues>
      <part-of>TENANTS</part-of>
   </resource-quota>
</resource-quotas>

<address-settings>
   <address-setting match="tenant.#">
      <resource-quota>tenant.*</resource-quota>
   </address-setting>
</address-settings>
```

### Geographic Segmentation

```xml
<resource-quotas>
   <!-- Regional quotas -->
   <resource-quota name="AMERICAS">
      <max-message-bytes>20G</max-message-bytes>
   </resource-quota>

   <resource-quota name="EUROPE">
      <max-message-bytes>15G</max-message-bytes>
   </resource-quota>

   <resource-quota name="APAC">
      <max-message-bytes>10G</max-message-bytes>
   </resource-quota>

   <!-- Country templates within regions -->
   <resource-quota name="US.*">
      <max-message-bytes>5G</max-message-bytes>
      <part-of>AMERICAS</part-of>
   </resource-quota>

   <resource-quota name="EU.*">
      <max-message-bytes>3G</max-message-bytes>
      <part-of>EUROPE</part-of>
   </resource-quota>
</resource-quotas>

<address-settings>
   <address-setting match="us.#">
      <resource-quota>US.*</resource-quota>
   </address-setting>

   <address-setting match="eu.#">
      <resource-quota>EU.*</resource-quota>
   </address-setting>
</address-settings>
```

### Department Quotas

```xml
<resource-quotas>
   <!-- Department quotas without hierarchy -->
   <resource-quota name="dept.engineering">
      <max-message-bytes>10G</max-message-bytes>
      <max-queues>1000</max-queues>
   </resource-quota>

   <resource-quota name="dept.sales">
      <max-message-bytes>5G</max-message-bytes>
      <max-queues>500</max-queues>
   </resource-quota>

   <resource-quota name="dept.support">
      <max-message-bytes>2G</max-message-bytes>
      <max-queues>200</max-queues>
   </resource-quota>
</resource-quotas>

<address-settings>
   <address-setting match="eng.#">
      <resource-quota>dept.engineering</resource-quota>
   </address-setting>

   <address-setting match="sales.#">
      <resource-quota>dept.sales</resource-quota>
   </address-setting>

   <address-setting match="support.#">
      <resource-quota>dept.support</resource-quota>
   </address-setting>
</address-settings>
```

## Differences from Global Limits

| Feature | Resource Quotas | Global Max Size |
|---------|----------------|-----------------|
| Scope | Per quota (address namespace) | Entire broker |
| Hierarchy | Parent-child relationships | Single global limit |
| Granularity | Multiple quotas with different limits | One limit for all |
| Isolation | Quotas independent of each other | All addresses share limit |
| Configuration | quota + address-settings | global-max-size |
| Enforcement | Per-quota basis | Broker-wide |
| Use case | Multi-tenant, departmental | Simple capacity control |

Resource quotas and global limits are **independent**. An address can be subject to both:
- Its resource quota limits (if configured)
- The broker's global-max-size limit

Both limits are enforced; the stricter limit applies.

## Limitations

1. **No runtime quota creation**: Quotas must be defined in `broker.xml` (except wildcard instances)
2. **No dynamic limit changes**: Changing limits requires broker restart
3. **No per-user quotas**: Quotas apply to addresses, not individual users/connections
4. **Wildcard extraction is simple**: Only handles single wildcard at one position
5. **No quota deletion**: Quota instances persist for broker lifetime
6. **No quota statistics API**: JMX metrics implementation pending

## Future Enhancements

- Management API for runtime quota CRUD operations
- JMX MBeans for quota monitoring
- Per-connection or per-user quotas
- Token bucket rate limiting within quotas
- More sophisticated wildcard matching
- Quota instance lifecycle management (cleanup)
- Integration with broker plugins for custom quota logic
