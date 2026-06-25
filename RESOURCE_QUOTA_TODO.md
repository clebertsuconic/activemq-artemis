# Big Rock
 - rebuild bytes quota on restart
 - deal with persistence size, queues accumulate the encoded size, we could track that but possibly the page estimate would be better, but pageSize is configurable so that gets in the way of consistenty but could be accounted for i guess.
the quota maxBytes needs to be paging independent. "2GB" can be in memory or on disk, the quota applies to both.
 - A broker could be non persistent or paging always and the quota needs to make sense in both cases.

# Small tidy
## Address token 
 - based around addAddressInfo - may be better around the page store. we just care when one is created, not on any errors that can ocurr during creation... we could link into the jmx registration or page store creation

Same for Queue token, it could possibly be simpler - see the effect. Dealing with rollback may not be necessar if we work on some afterX callback or event.

We may not need a service/component, no state for stop/start.

Can remove the callbacks and simplify a bit because we don't deal with block/unblock

## global max

No integration with global max, but to do it would introduce quota as first class citizen, may be too much of an overhead. But is a consistent arch.
Quota part of global (globalMax etc), so there is a globalQuota that is the parent of all. In the absence of a parent partOf.
This needs some thought.
the current quota parent is independent of global size. it is not involved in paging of full policy, and that is good. But it could be aware of globalMax.

