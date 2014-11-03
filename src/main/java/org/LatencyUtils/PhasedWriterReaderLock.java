/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link PhasedWriterReaderLock} provides an asymmetric means for synchronizing wait-free "writer" critical
 * section execution against a "reader phase flip" that needs to make sure no writer critical
 * sections that were active at the beginning of the flip are still active after the flip is done.
 * <p>
 * {@link PhasedWriterReaderLock} "writers" are wait free, "readers" block for other "readers", and
 * "readers" are only blocked by "writers" that preceded their {@link PhasedWriterReaderLock#flipPhase()} attempt.
 * <p>
 * When used to protect an actively recording data structure, the assumptions on how readers and writers act are:
 * <ol>
 * <li>There are two data structures ("active" and "inactive")</li>
 * <li>Writing is done to the perceived active version (as perceived by the writer), and only
 *     when holding a writerLock.</li>
 * <li>Only readers switch the perceived roles of the active and inactive data structures.
 *     They do so only while under readerLock(), and only before calling flipPhase().</li>
 * <li>When assumptions 1-3 are met, PhasedWriteReadLock guarantees that the inactive version is
 *     not being modified while being read while under readerLock() protection after a flipPhase() operation.</li>
 * </ol>
 */
public class PhasedWriterReaderLock {
    private volatile long startEpoch = 0;
    private volatile long evenEndEpoch = 0;
    private volatile long oddEndEpoch = 1;

    private final ReentrantLock readerLock = new ReentrantLock();

    private static final AtomicLongFieldUpdater<PhasedWriterReaderLock> startEpochUpdater =
            AtomicLongFieldUpdater.newUpdater(PhasedWriterReaderLock.class, "startEpoch");
    private static final AtomicLongFieldUpdater<PhasedWriterReaderLock> evenEndEpochUpdater =
            AtomicLongFieldUpdater.newUpdater(PhasedWriterReaderLock.class, "evenEndEpoch");
    private static final AtomicLongFieldUpdater<PhasedWriterReaderLock> oddEndEpochUpdater =
            AtomicLongFieldUpdater.newUpdater(PhasedWriterReaderLock.class, "oddEndEpoch");

    /**
     * Indicate entry to a critical section containing a write operation.
     * <p>
     * This call is wait-free on architectures that support wait free atomic add operations,
     * and is lock-free on architectures that only support atomic CAS or SWAP operations.
     * <p>
     * {@link PhasedWriterReaderLock#writerLock()} must be matched with a subsequent
     * {@link PhasedWriterReaderLock#writerLock} in order for CriticalSectionPhaser
     * synchronization to function properly.
     *
     * @return an (opaque) value associated with the critical section entry, which MUST be provided to the matching
     * {@link PhasedWriterReaderLock#writerLock} call.
     */
    public long writerLock() {
        return startEpochUpdater.getAndAdd(this, 2);
    }

    /**
     * Indicate exit from a critical section containing a write operation.
     * <p>
     * This call is wait-free on architectures that support wait free atomic add operations,
     * and is lock-free on architectures that only support atomic CAS or SWAP operations.
     * <p>
     * {@link PhasedWriterReaderLock#writerLock} must be matched with a preceding
     * {@link PhasedWriterReaderLock#writerLock()} call, and must be provided with the
     * matching {@link PhasedWriterReaderLock#writerLock()} call's return value, in
     * order for CriticalSectionPhaser synchronization to function properly.
     *
     * @param criticalValueAtEnter the (opaque) value returned from the matching
     * {@link PhasedWriterReaderLock#writerLock()} call.
     */
    public void writerUnlock(long criticalValueAtEnter) {
        if ((criticalValueAtEnter & 1) == 0) {
            evenEndEpochUpdater.getAndAdd(this, 2);
        } else {
            oddEndEpochUpdater.getAndAdd(this, 2);
        }
    }

    /**
     * Indicate entry to a critical section containing a read operation.
     */
    public void readerLock() {
        readerLock.lock();
    }

    /**
     * Indicate exit from a critical section containing a read operation.
     */
    public void readerUnlock() {
        readerLock.unlock();
    }

    /**
     * Flip a phase in the {@link PhasedWriterReaderLock} instance.
     * {@link PhasedWriterReaderLock#flipPhase()} will return only after all writer critical sections (protected by
     * {@link org.LatencyUtils.PhasedWriterReaderLock#writerLock()} ()} and
     * {@link PhasedWriterReaderLock#writerUnlock(long)} ()}) that may have been in flight when the
     * {@link PhasedWriterReaderLock#flipPhase()} call were made had completed.
     * <p>
     * No actual writer critical section activity is required for {@link PhasedWriterReaderLock#flipPhase()} to
     * succeed.
     * <p>
     * {@link PhasedWriterReaderLock#flipPhase()} can only be called while holding the readerLock().
     * <p>
     * However, {@link PhasedWriterReaderLock#flipPhase()} is lock-free with respect to calls to
     * {@link PhasedWriterReaderLock#writerLock()} and
     * {@link PhasedWriterReaderLock#writerUnlock(long)}. It may spin-wait for for active
     * writer critical section code to complete.
     */
    public void flipPhase() {
        if (!readerLock.isHeldByCurrentThread()) {
            throw new IllegalStateException("flipPhase() can only be called while holding the readerLock()");
        }

        boolean nextPhaseIsOdd = ((startEpoch & 1) == 0);

        long initialStartValue;
        // First, clear currently unused [next] phase end epoch (to proper initial value for phase):
        if (nextPhaseIsOdd) {
            oddEndEpoch = initialStartValue = 1;
        } else {
            evenEndEpoch = initialStartValue = 0;
        }

        // Next, reset start value, indicating new phase, and retain value at flip:
        long startValueAtFlip = startEpochUpdater.getAndSet(this, initialStartValue);

        // Now, spin until previous phase end value catches up with start value at flip:
        boolean caughtUp = false;
        do {
            if (nextPhaseIsOdd) {
                caughtUp = (evenEndEpoch == startValueAtFlip);
            } else {
                caughtUp = (oddEndEpoch == startValueAtFlip);
            }
        } while (!caughtUp);
    }
}
