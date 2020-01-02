package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// TODO: Verification of concurrent code by some model checker?
// NOTE: RLock to WLock upgrade is only safe on a thread-bound transaction.
// LockingTest#acquireReadWriteLocksOnSamePage seems to be problematic.

/**
 * LockManager keeps track of which locks each transaction holds and checks to see if a lock should be granted to a
 * transaction when it is requested.
 */
public class LockManager {
    private class PageLock {
        private final LockManager manager;
        final PageId pageId;
        Permissions permissions;
        final HashSet<TransactionId> lockedBy;
        final HashSet<TransactionId> waitedBy;

        PageLock(LockManager manager, PageId pid, TransactionId tid, Permissions perm) {
            this.manager = manager;
            this.pageId = pid;
            this.permissions = perm;
            HashSet<TransactionId> s = new HashSet<>();
            s.add(tid);
            this.lockedBy = s;
            this.waitedBy = new HashSet<>();
        }

        private boolean canAcquire(TransactionId tid, Permissions perm) {
            if (this.permissions == Permissions.READ_ONLY) {
                if (perm == Permissions.READ_ONLY) {
                    return true;
                } else if (perm == Permissions.READ_WRITE) {
                    // Whether the read lock is solely hold by the same transaction
                    return this.lockedBy.size() == 1 && this.lockedBy.contains(tid);
                    // The RLock -> WLock upgrade is done implicitly
                }
            } else if (this.permissions == Permissions.READ_WRITE) {
                // Whether the write lock is hold by the same transaction
                return this.lockedBy.contains(tid);
            }
            return true;
        }

        synchronized void lock(TransactionId tid, Permissions perm)
                throws InterruptedException, TransactionAbortedException {
            while (!canAcquire(tid, perm)) {
                this.manager.addToWaitForGraph(tid, this);
                this.waitedBy.add(tid);
                if (this.manager.detectDeadLock(tid, this)) {
                    this.lockedBy.remove(tid);
                    this.waitedBy.remove(tid);
                    this.manager.removeFromWaitForGraph(tid, this.waitedBy);
                    notifyAll();
                    throw new TransactionAbortedException();
                }
                wait();
            }

            this.permissions = perm;
            this.lockedBy.add(tid);
            this.waitedBy.remove(tid);
            // There may be some transactions waiting for exclusive lock.
            for (TransactionId waiter : this.waitedBy) {
                this.manager.addToWaitForGraph(waiter, this);
            }
            this.manager.txnLockingMapAdd(tid, this.pageId);
        }

        synchronized void unlock(TransactionId tid) {
            this.lockedBy.remove(tid);
            if (this.lockedBy.isEmpty()) {
                this.permissions = null;
                // Could remove the page from `lockMap`, but it would introduce lots of race conditions.
            }
            this.manager.removeFromWaitForGraph(tid, this.waitedBy);
            this.manager.txnLockingMapRemove(tid, this.pageId);
            notifyAll();
        }
    }

    private ConcurrentHashMap<PageId, PageLock> lockMap;
    private HashMap<TransactionId, HashSet<PageId>> txnLockingMap;
    private final HashMap<TransactionId, HashSet<TransactionId>> waitForGraph;

    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>();
        this.txnLockingMap = new HashMap<>();
        this.waitForGraph = new HashMap<>();
    }

    private synchronized void txnLockingMapAdd(TransactionId tid, PageId pid) {
        HashSet<PageId> s = this.txnLockingMap.get(tid);
        if (s == null) {
            s = new HashSet<>();
            this.txnLockingMap.put(tid, s);
        }
        s.add(pid);
    }

    private synchronized void txnLockingMapRemove(TransactionId tid, PageId pid) {
        HashSet<PageId> s = this.txnLockingMap.get(tid);
        if (s == null) {
            return;
        }
        s.remove(pid);
        if (s.isEmpty()) {
            this.txnLockingMap.remove(tid);
        }
    }

    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        PageLock pageLock = this.lockMap.get(pid);
        if (pageLock == null) {
            pageLock = new PageLock(this, pid, tid, perm);
            PageLock previous = this.lockMap.putIfAbsent(pid, pageLock);
            if (previous != null) {
                pageLock = previous;
            }
        }

        try {
            pageLock.lock(tid, perm);
        } catch (InterruptedException e) {
            throw new TransactionAbortedException();
        }
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        PageLock pageLock = this.lockMap.get(pid);
        if (pageLock != null) {
            pageLock.unlock(tid);
        }
    }

    private synchronized void addToWaitForGraph(TransactionId fromNode, PageLock pageLock) {
        HashSet<TransactionId> toNodes = this.waitForGraph.get(fromNode);
        if (toNodes == null) {
            toNodes = new HashSet<>();
            this.waitForGraph.put(fromNode, toNodes);
        }
        for (TransactionId t : pageLock.lockedBy) {
            if (!t.equals(fromNode)) {
                toNodes.add(t);
            }
        }
    }

    private synchronized void removeFromWaitForGraph(TransactionId tid, Iterable<TransactionId> waiters) {
        this.waitForGraph.remove(tid);
        for (TransactionId waiter : waiters) {
            HashSet<TransactionId> waiting = this.waitForGraph.get(waiter);
            if (waiting != null) {
                waiting.remove(tid);
            }
        }
    }

    // Draws DOT graph for debugging.
    private synchronized void drawWaitForGraph() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n================\n");
        sb.append("digraph wfg {\n");
        for (Map.Entry<TransactionId, HashSet<TransactionId>> entry : this.waitForGraph.entrySet()) {
            TransactionId from = entry.getKey();
            for (TransactionId to : entry.getValue()) {
                sb.append(String.format("\t%d -> %d;\n", from.getId(), to.getId()));
            }
        }
        sb.append("}\n\n");
        System.out.print(sb);
    }

    private synchronized boolean detectDeadLock(TransactionId tid, PageLock pageLock) {
        // Checks whether any node in `pageLock.lockedBy` is connected to `tid`.
        HashSet<TransactionId> visited = new HashSet<>();
        Queue<TransactionId> q = new ArrayDeque<>(pageLock.lockedBy);
        while (!q.isEmpty()) {
            TransactionId head = q.poll();
            HashSet<TransactionId> toNodes = this.waitForGraph.get(head);
            if (toNodes != null) {
                for (TransactionId child : toNodes) {
                    if (!visited.contains(child)) {
                        // Checks here because `tid` may belong to `pageLock.lockedBy`.
                        if (child.equals(tid)) {
                            return true;
                        }
                        q.add(child);
                        visited.add(child);
                    }
                }
            }
        }
        return false;
    }

    public synchronized boolean txnHoldsLock(TransactionId tid, PageId pid) {
        HashSet<PageId> s = this.txnLockingMap.get(tid);
        return s != null && s.contains(pid);
    }

    // (!) Do not make the method `synchronized` or it will deadlock.
    public void txnReleaseLocks(TransactionId tid) {
        Iterator<PageId> it;
        synchronized (this) {
            HashSet<PageId> s = this.txnLockingMap.remove(tid);
            if (s == null) {
                return;
            }
            it = s.iterator();
        }

        while (it.hasNext()) {
            PageId pid = it.next();
            it.remove(); // prevents ConcurrentModificationException
            releaseLock(tid, pid);
        }
    }
}
