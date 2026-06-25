# Resource Quota Architecture

## Overview

This document describes the internal architecture of the resource quota feature, explaining how quotas are loaded, managed, enforced, and how counters are rebuilt after broker restart.

## Component Overview

### Core Classes

```
ResourceQuotaConfig          - Configuration (limits only, serializable)
ResourceQuota                - Runtime tracker (limits + counters, NOT serializable)
ResourceQuotaManager         - Template instantiation and parent resolution
ResourceQuotaService         - Token acquisition and quota enforcement
QuotaToken                   - Auto-rollback token for transactional quota ops
AddressQuotaToken            - Token for address count increments
QueueQuotaToken              - Token for queue count increments
```

### Class Relationships

```
Configuration
  └─ Map<String, ResourceQuotaConfig>  (from broker.xml)
         │
         └─> createRuntimeQuota() 
                │
                ▼
            ResourceQuota  (runtime instance, not serialized)
                │
                ├─ SizeAwareMetric (byte tracking)
                ├─ AtomicInteger addressCount
                ├─ AtomicInteger queueCount
                └─ ResourceQuota parent (reference)

ResourceQuotaManager
  ├─ HierarchicalObjectRepository<ResourceQuota>  (quota lookup)
  └─ ConcurrentHashMap<String, ResourceQuota>     (wildcard instances)

ResourceQuotaService
  ├─ ResourceQuotaManager
  └─ AddressSettingsRepository  (quota name lookup)
```

## Lifecycle

### 1. Broker Startup

#### Configuration Loading

`FileConfigurationParser.parseResourceQuotas()`:
1. Parse `<resource-quotas>` section from `broker.xml`
2. Create `ResourceQuotaConfig` instances (limits only)
3. Add to `Configuration.resourceQuotaConfigs` map
4. Parse `<resource-quota>` references in address-settings

#### Runtime Initialization

`ActiveMQServerImpl.initialisePart2()`:
1. Create `ResourceQuotaManager` with `HierarchicalObjectRepository`
2. For each `ResourceQuotaConfig`:
   - Call `config.createRuntimeQuota()` to create `ResourceQuota` instance
   - Add to `ResourceQuotaManager` repository
3. Call `resourceQuotaManager.establishParentRelationships()`
   - Resolves `partOf` references to actual parent `ResourceQuota` objects
   - Detects circular references
   - Builds parent chain recursively
4. Store `ResourceQuotaManager` in `PagingManager`
5. Create `ResourceQuotaService` with manager and address-settings repository

At this point:
- All quota **limits** are loaded
- All quota **counters** are zero
- Parent relationships are established
- Broker is ready to rebuild counters during journal replay

### 2. Journal Replay (Counter Rebuild)

#### Address Counter Rebuild

`PostOfficeJournalLoader.handleAddAddress()`:
1. Create `AddressInfo` from journal record
2. Call `postOffice.addAddressInfo(addressInfo, true)`
   - Pass `reloaded = true` flag
3. `PostOfficeImpl.addAddressInfo()`:
   - If `reloaded = true`, skip quota limit check
   - Call `pagingManager.addAddress(addressInfo, reloaded)`
4. `PagingStoreImpl.incrementAddressCount()`:
   - Get quota via `resourceQuotaManager.getQuotaForAddress()`
   - Call `quota.incrementAddressCount()` (without limit check)
   - Propagates up parent chain

#### Queue Counter Rebuild

`PostOfficeJournalLoader.handleAddBinding()`:
1. Create `QueueBinding` from journal record
2. Call `postOffice.addBinding(binding, true)`
   - Pass `reloaded = true` flag
3. `PostOfficeImpl.addBinding()`:
   - If `reloaded = true`, skip quota limit check
   - Call `pagingManager.addQueue(addressInfo, queueConfig, reloaded)`
4. `PagingStoreImpl.incrementQueueCount()`:
   - Get quota via `resourceQuotaManager.getQuotaForAddress()`
   - Call `quota.incrementQueueCount()` (without limit check)
   - Propagates up parent chain

#### Message Byte Counter Rebuild

`PostOfficeJournalLoader.handleAddMessage()`:
1. Decode message from journal
2. Add to page store via `pagingStore.addSize()`
3. If quota exists, call `quota.addSize(msgSize, sizeOnly=true)`
   - `sizeOnly=true` prevents double-counting in `SizeAwareMetric` elements
   - Propagates to parent via `onSizeCallback`

After journal replay:
- All address counters reflect actual persisted addresses
- All queue counters reflect actual persisted queues
- All byte counters reflect actual persisted message data
- Quota enforcement can begin

### 3. Runtime Operation

#### Address Creation (New Address)

```java
// PostOfficeImpl.addAddressInfo()
try (AddressQuotaToken token = quotaService.acquireAddressToken(addressInfo, reloaded)) {
   // Perform address creation...
   pagingManager.addAddress(addressInfo, reloaded);
   
   // If everything succeeds, commit the quota increment
   token.commit();
} // Auto-rollback if exception or no commit
```

`ResourceQuotaService.acquireAddressToken()`:
1. Lookup address-settings for address
2. Get quota name from settings.getResourceQuota()
3. Resolve quota via `resourceQuotaManager.getQuotaForAddress()`
   - If wildcard, instantiate template
   - Otherwise direct lookup
4. Create `AddressQuotaTokenImpl` with quota
5. Token constructor calls `quota.tryIncrementAddressCount()`
   - Atomically checks limit and increments counter
   - Propagates to parent chain
   - Returns false if any limit exceeded
6. If limit exceeded, throw `ActiveMQResourceQuotaExceededException`
7. Return token

Token lifecycle:
- **commit()**: Marks token as committed, prevents rollback
- **close()**: If not committed, calls `quota.decrementAddressCount()` to rollback

#### Queue Creation (New Queue)

Same pattern as address creation using `QueueQuotaToken`:

```java
try (QueueQuotaToken token = quotaService.acquireQueueToken(addressInfo, queueConfig, reloaded)) {
   // Create queue...
   token.commit();
}
```

#### Message Byte Tracking

Message byte quotas work differently from address/queue quotas - they don't use a token pattern. Instead, bytes are tracked directly via the paging store:

**When messages are added:**
```java
// PagingStoreImpl.addSize()
public void addSize(final int size, boolean sizeOnly, boolean affectGlobal) {
   // Track quota alongside paging store size
   final ResourceQuota quota = resourceQuota;
   if (quota != null && size != 0) {
      quota.addSize(size, true);  // Directly update quota counter
   }
   
   long newSize = this.size.addSize(size, sizeOnly, affectGlobal);
   // ... paging logic ...
}
```

**When checking limits:**
```java
// PagingStoreImpl.checkMemory()
final ResourceQuota quota = resourceQuota;
if (quota != null && quota.isOverByteLimit()) {
   throw ActiveMQResourceQuotaExceededException(
      "quota '" + quota.getName() + "' exceeded - current: " + 
      quota.getCurrentMessageBytes() + " bytes, max: " + 
      quota.getMaxMessageBytes() + " bytes");
}
```

Key differences from address/queue quotas:
- **No token pattern**: Bytes are tracked directly, not via acquire/commit
- **Check happens in checkMemory()**: Limit is checked when routing messages, not when acquiring a token
- **Always throws exception**: Unlike address-full-policy which can PAGE or DROP, quota byte limit always throws
- **Propagates to parent**: Size deltas automatically propagate up the parent chain via SizeAwareMetric callback

### 4. Resource Deletion

#### Address Deletion

```java
// PostOfficeImpl.removeAddressInfo()
try (AddressRemovalToken token = quotaService.acquireAddressRemovalToken(addressInfo)) {
   // Remove address...
   token.commit();
}
```

`AddressRemovalToken` decrements counters on commit (inverse of creation):
- **constructor**: No-op (no quota check needed for deletion)
- **commit()**: Calls `quota.decrementAddressCount()`
- **close()**: No-op (deletion committed or not - no rollback needed)

#### Queue Deletion

Same pattern with `QueueRemovalToken`.

## Wildcard Template Resolution

### Template Matching

When `AddressSettings.resourceQuota` contains `*`:

```java
// ResourceQuotaManager.getQuotaForAddress()
if (quotaName.contains("*")) {
   return resolveWildcardQuota(quotaName, address);
}
```

### Instance Creation

`ResourceQuotaManager.resolveWildcardQuota()`:

1. **Look up template**: 
   ```java
   ResourceQuota template = quotaRepository.getMatch(quotaTemplate);
   ```

2. **Extract wildcard value**:
   ```java
   // For quota "EU.*" and address "eu.fr.orders"
   String[] addressParts = "eu.fr.orders".split("\\.");      // [eu, fr, orders]
   String[] templateParts = "EU.".split("\\.");              // [EU]
   int wildcardIndex = templateParts.length;                  // 1
   String wildcardValue = addressParts[wildcardIndex];       // "fr"
   ```

3. **Build instance name**:
   ```java
   String instanceName = quotaTemplate.replace("*", wildcardValue);  // "EU.fr"
   ```

4. **Create or reuse instance**:
   ```java
   return instantiatedQuotas.computeIfAbsent(instanceName, name -> {
      ResourceQuota instance = template.copy(name);
      if (instance.getPartOf() != null) {
         ResourceQuota parent = quotaRepository.getMatch(instance.getPartOf());
         instance.setParent(parent);
      }
      return instance;
   });
   ```

5. **Return instance**: Future requests for the same instance name reuse the cached instance

### Instance Lifecycle

- Created lazily on first matching address
- Cached in `ResourceQuotaManager.instantiatedQuotas` for broker lifetime
- Counters rebuild on restart like regular quotas
- No cleanup/removal mechanism (future enhancement)

## Parent Chain Enforcement

### Hierarchy Establishment

`ResourceQuotaManager.establishParentChain()`:

```java
private void establishParentChain(ResourceQuota quota, Map<String, ResourceQuota> allQuotas, 
                                   Set<String> visited) {
   if (quota.getPartOf() == null || quota.getParent() != null) {
      return;  // No parent or already processed
   }
   
   if (visited.contains(quota.getName())) {
      logger.error("Circular reference detected");
      return;
   }
   
   visited.add(quota.getName());
   
   ResourceQuota parent = allQuotas.get(quota.getPartOf());
   establishParentChain(parent, allQuotas, visited);  // Recursive
   
   quota.setParent(parent);
   visited.remove(quota.getName());
}
```

Result: Every quota with `partOf` has its `parent` reference set to the actual `ResourceQuota` object.

### Increment Propagation

When quota counter is incremented:

```java
// ResourceQuota.incrementAddressCount()
public void incrementAddressCount() {
   addressCount.incrementAndGet();
   if (parent != null) {
      parent.incrementAddressCount();  // Recursive propagation
   }
}
```

Result: Increment walks up the parent chain, incrementing each ancestor's counter.

### Limit Check Propagation

When checking limits atomically:

```java
// ResourceQuota.tryIncrementAddressCount()
public boolean tryIncrementAddressCount() {
   boolean parentIncremented = false;
   try {
      // Check parent first (parent limit is more restrictive)
      if (parent != null) {
         if (!parent.tryIncrementAddressCount()) {
            return false;  // Parent limit exceeded
         }
         parentIncremented = true;
      }
      
      // Atomically check and increment self using CAS loop
      while (true) {
         int current = addressCount.get();
         if (maxAddresses >= 0 && current >= maxAddresses) {
            return false;  // Self limit exceeded
         }
         if (addressCount.compareAndSet(current, current + 1)) {
            parentIncremented = false;  // Success, don't rollback
            return true;
         }
      }
   } finally {
      // Rollback parent if we incremented it but failed to increment self
      if (parentIncremented && parent != null) {
         parent.decrementAddressCount();
      }
   }
}
```

Result: Check walks up parent chain first, then increments atomically with rollback on failure.

## Token Pattern Implementation

### Interface Definition

```java
public interface QuotaToken extends AutoCloseable {
   void commit();
   void close();
}
```

### Implementation Pattern

```java
public class AddressQuotaTokenImpl implements AddressQuotaToken {
   private final ResourceQuota quota;
   private boolean committed = false;

   public AddressQuotaTokenImpl(ResourceQuota quota) throws ActiveMQException {
      this.quota = quota;
      if (quota != null && !quota.tryIncrementAddressCount()) {
         throw new ActiveMQResourceQuotaExceededException("Address quota exceeded");
      }
   }

   @Override
   public void commit() {
      committed = true;
   }

   @Override
   public void close() {
      if (!committed && quota != null) {
         quota.decrementAddressCount();  // Rollback
      }
   }
}
```

### Usage Pattern

```java
try (AddressQuotaToken token = quotaService.acquireAddressToken(addressInfo, false)) {
   // If we reach here, quota was successfully incremented
   
   // ... perform operations that might throw ...
   
   createAddress(...);
   
   // Success - prevent rollback
   token.commit();
   
} catch (Exception e) {
   // Token auto-closes here without commit() - quota is rolled back
   throw e;
}
```

### Benefits

1. **Automatic cleanup**: `try-with-resources` ensures `close()` called
2. **Exception safety**: Rollback happens automatically on exception
3. **Explicit commit**: Must call `commit()` to keep quota increment
4. **No manual rollback**: No need for `finally` blocks
5. **Composable**: Multiple tokens can be acquired and composed

## Transactional Integration

### Transaction Participation

When quota tokens are used in transactions, they must roll back if the transaction rolls back.

`QuotaTransactionOperation`:
```java
public class QuotaTransactionOperation extends TransactionOperationAbstract {
   private final QuotaToken token;
   
   @Override
   public void afterCommit(Transaction tx) {
      token.commit();  // Transaction committed - commit quota
   }
   
   @Override
   public void afterRollback(Transaction tx) {
      token.close();   // Transaction rolled back - rollback quota
   }
}
```

Usage:
```java
QuotaToken token = quotaService.acquireAddressToken(addressInfo, false);
transaction.addOperation(new QuotaTransactionOperation(token));
// Token commit/rollback now tied to transaction outcome
```

## SizeAwareMetric Integration

### Byte Tracking

`ResourceQuota` uses `SizeAwareMetric` for byte accounting:

```java
private SizeAwareMetric sizeMetric;

private void initializeRuntimeState() {
   long maxBytes = getMaxMessageBytes();
   long lowerMarkBytes = maxBytes > 0 ? (long) (maxBytes * 0.9) : -1;
   
   this.sizeMetric = new SizeAwareMetric(maxBytes, lowerMarkBytes, -1, -1);
   
   // Propagate size changes to parent
   this.sizeMetric.setOnSizeCallback((delta, sizeOnly) -> {
      if (parent != null) {
         parent.addSize(delta, sizeOnly);
      }
   });
}
```

### Size Delta Propagation

When bytes are added to a quota:

```java
// ResourceQuota.addSize()
public long addSize(int delta, boolean sizeOnly) {
   return sizeMetric.addSize(delta, sizeOnly);
   // sizeMetric invokes onSizeCallback which propagates to parent
}
```

Result: Size deltas automatically propagate up the parent chain via callback.

### Over/Under Callbacks

Quotas can set callbacks for over/under transitions:

```java
quota.setOverCallback(() -> {
   logger.warn("Quota {} over limit", quota.getName());
   // Could trigger alerts, metrics, etc.
});

quota.setUnderCallback(() -> {
   logger.info("Quota {} under limit", quota.getName());
});
```

Currently these are unused but available for future enhancements (paging control, alerts, etc).

## Address Settings Integration

### Quota Name Lookup

`ResourceQuotaService` resolves quota names from address-settings:

```java
public AddressQuotaToken acquireAddressToken(AddressInfo addressInfo, boolean reloaded) {
   AddressSettings settings = addressSettingsRepository.getMatch(addressInfo.getName().toString());
   if (settings == null || settings.getResourceQuota() == null) {
      return NoOpToken.INSTANCE;  // No quota configured
   }
   
   ResourceQuota quota = resourceQuotaManager.getQuotaForAddress(addressInfo.getName(), settings);
   return new AddressQuotaTokenImpl(quota, reloaded);
}
```

### Match Order

Address-settings match using `HierarchicalRepository` with longest-prefix-match:
1. Most specific pattern wins
2. Wildcards (`#`, `*`) supported
3. First match determines quota name
4. Quota name then resolved to actual quota

Example:
```xml
<address-setting match="eu.fr.#">
   <resource-quota>EU.fr</resource-quota>
</address-setting>

<address-setting match="eu.#">
   <resource-quota>EU.*</resource-quota>
</address-setting>
```

Address `eu.fr.orders` matches `eu.fr.#` first → uses quota `EU.fr`.

## Error Handling

### Configuration Errors

**Missing parent reference**:
```java
// ResourceQuotaManager.establishParentChain()
if (parent == null) {
   logger.warn("Parent quota {} not found for quota {}", parentName, quota.getName());
   return;  // Continue without parent - quota still usable
}
```

**Circular parent reference**:
```java
if (visited.contains(quota.getName())) {
   logger.error("Circular parent reference detected for quota: {}", quota.getName());
   return;  // Break cycle - quota usable without parent
}
```

**Duplicate quota name**:
```java
if (config.getResourceQuotaConfigs().containsKey(quotaConfig.getName())) {
   logger.warn("Duplicate resource quota name: {}", quotaConfig.getName());
} else {
   config.addResourceQuotaConfig(quotaConfig.getName(), quotaConfig);
}
```

### Runtime Errors

**Quota limit exceeded**:
```java
throw new ActiveMQResourceQuotaExceededException(
   "Address quota exceeded for " + quota.getName()
);
```

**Wildcard template not found**:
```java
ResourceQuota template = quotaRepository.getMatch(quotaTemplate);
if (template == null) {
   logger.warn("Quota template {} not found", quotaTemplate);
   return null;  // No quota enforcement
}
```

**Counter goes negative** (indicates bug):
```java
int current = addressCount.decrementAndGet();
if (current < 0) {
   logger.warn("Quota {} address count went negative: {}", name, current);
   addressCount.set(0);  // Reset to zero
   return;  // Don't propagate to parent
}
```

## Concurrency

### Thread Safety

All quota operations are thread-safe:

- **Counter increments**: `AtomicInteger.compareAndSet()` loops
- **Size tracking**: `SizeAwareMetric` internal synchronization
- **Instance creation**: `ConcurrentHashMap.computeIfAbsent()`
- **Parent propagation**: Atomic propagation via CAS, rollback on failure

### Race Condition Handling

**Atomic check-and-increment**:
```java
while (true) {
   int current = addressCount.get();
   if (maxAddresses >= 0 && current >= maxAddresses) {
      return false;  // Limit check
   }
   if (addressCount.compareAndSet(current, current + 1)) {
      return true;  // Success
   }
   // Loop if another thread modified count
}
```

This prevents TOCTOU (time-of-check-time-of-use) races where limit could be exceeded.

**Parent rollback**:
```java
boolean parentIncremented = false;
try {
   if (parent != null) {
      if (!parent.tryIncrementAddressCount()) return false;
      parentIncremented = true;
   }
   // ... increment self ...
   parentIncremented = false;  // Success
   return true;
} finally {
   if (parentIncremented && parent != null) {
      parent.decrementAddressCount();  // Rollback parent on self failure
   }
}
```

## Performance Considerations

### Overhead

**Per address creation**:
- 1 quota lookup (hash table lookup)
- 1 atomic CAS per quota in hierarchy
- Parent chain walk (typically 1-3 levels)

**Per message routed**:
- 1 atomic add for byte increment
- Callback propagation up parent chain

**Per wildcard instance**:
- 1 wildcard extraction (string split + array index)
- 1 `computeIfAbsent` (creates only once, cached thereafter)

### Optimization Opportunities

1. **Cache quota in AddressInfo**: Avoid repeated lookups
2. **Batch counter updates**: Accumulate and flush periodically
3. **Lock-free parent propagation**: Current CAS-based approach is already lock-free
4. **Wildcard instance cleanup**: Remove unused instances to reduce memory

## Testing Strategy

### Unit Tests

- `ResourceQuotaTest`: Test limit enforcement, counter operations, parent propagation
- `ResourceQuotaManagerTest`: Test wildcard resolution, parent chain establishment
- `ResourceQuotaConfigTest`: Test configuration parsing, merging
- `QuotaTokenTest`: Test token lifecycle, commit/rollback semantics

### Integration Tests

- `ResourceQuotaIntegrationTest`: Test end-to-end with broker startup, address/queue creation
- `ResourceQuotaJournalReplayTest`: Test counter rebuild after restart
- `ResourceQuotaWildcardTest`: Test wildcard template instantiation
- `ResourceQuotaHierarchyTest`: Test parent-child enforcement

### Scenario Tests

- **Concurrent address creation**: Multiple threads creating addresses in same quota
- **Quota hierarchy**: Child quota hitting parent limit before self limit
- **Wildcard instantiation**: Multiple addresses triggering same template
- **Counter rebuild**: Restart broker, verify counters match pre-restart state
- **Exception rollback**: Verify counters rolled back when operations fail

## Future Enhancements

### Management API

Add runtime quota CRUD operations:
```java
// Create quota at runtime
server.createResourceQuota("new-quota", 1024L, 100, 500);

// Update quota limits
server.updateResourceQuota("existing-quota", newLimits);

// Delete quota (if no addresses using it)
server.removeResourceQuota("unused-quota");
```

### JMX Integration

Expose quota metrics via MBeans:
```java
@MBean
interface ResourceQuotaMBean {
   long getCurrentBytes();
   long getMaxBytes();
   int getCurrentAddresses();
   int getMaxAddresses();
   double getUtilizationPercent();
   String[] getChildQuotas();
}
```

### Advanced Wildcard Matching

Support multiple wildcards and more complex patterns:
- `region.*.tenant.*` → `region.us.tenant.acme`
- `{region}.{country}.#` → custom extraction logic

### Quota Pooling

Allow quotas to share a pool of resources:
```xml
<resource-quota name="shared-pool">
   <max-message-bytes>10G</max-message-bytes>
   <shared>true</shared>
</resource-quota>

<resource-quota name="tenant.a">
   <pool>shared-pool</pool>
</resource-quota>
```

### Rate Limiting

Add token bucket rate limiting within quotas:
```xml
<resource-quota name="rate-limited">
   <max-messages-per-second>1000</max-messages-per-second>
</resource-quota>
```

### Plugin Integration

Allow plugins to customize quota behavior:
```java
interface QuotaPlugin {
   boolean allowAddressCreation(AddressInfo address, ResourceQuota quota);
   void onQuotaExceeded(ResourceQuota quota, String reason);
}
```
