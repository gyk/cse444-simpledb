package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

        PageLock(LockManager manager, PageId pid, TransactionId tid, Permissions perm) {
            this.manager = manager;
            this.pageId = pid;
            this.permissions = perm;
            HashSet<TransactionId> s = new HashSet<>();
            s.add(tid);
            this.lockedBy = s;
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

        synchronized void lock(TransactionId tid, Permissions perm) throws InterruptedException {
            while (!canAcquire(tid, perm)) {
                wait();
            }

            this.permissions = perm;
            this.lockedBy.add(tid);
            this.manager.txnLockingMapAdd(tid, this.pageId);
        }

        synchronized void unlock(TransactionId tid) {
            this.lockedBy.remove(tid);
            if (this.lockedBy.isEmpty()) {
                this.permissions = null;
                // Could remove the page from `lockMap`, but it would introduce lots of race conditions.
            }
            this.manager.txnLockingMapRemove(tid, this.pageId);
            notifyAll();
        }
    }

    private ConcurrentHashMap<PageId, PageLock> lockMap;
    private HashMap<TransactionId, HashSet<PageId>> txnLockingMap;

    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>();
        this.txnLockingMap = new HashMap<>();
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
        pageLock.unlock(tid);
    }

    public synchronized boolean txnHoldsLock(TransactionId tid, PageId pid) {
        HashSet<PageId> s = this.txnLockingMap.get(tid);
        return s != null && s.contains(pid);
    }

    public synchronized void txnReleaseLocks(TransactionId tid) {
        HashSet<PageId> s = this.txnLockingMap.getOrDefault(tid, new HashSet<>());
        Iterator<PageId> it = s.iterator();
        while (it.hasNext()) {
            PageId pid = it.next();
            it.remove(); // prevents ConcurrentModificationException
            releaseLock(tid, pid);
        }
    }
}
