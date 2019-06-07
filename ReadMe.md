# MoDB #

A distributed key/value store with (operation-based) CRDTs built-in.

## No Master - All Nodes are the Same ##

All nodes in the cluster look the same, are the same, and act the same. You can
query any node or send updates to any node. This makes deployments easier to
manage and easier to look after.

## Our CRDTs are Commutative Replicated Data Types (CmRDTs) ##

We replicate all operations around the cluster such that we can be sure it
won't disappear. Once those operations are propagated sufficiently, they are
applied to their key and the result is determined.

Since we need to propogate operations (which takes non-zero time) we can also
provide two different types of reads. Either a "known" state where all nodes
agree (both history and value), or a "pending" state where one node seems to
know more than any other.

We don't replicate state around the cluster since that is less efficient than
just propagating operations. These CRDTs are known as Convergent Replicated
Data Types (CvRDTs). MoDB doesn't use these at all.

(Ends)
