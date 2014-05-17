/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Simple PauseDetector that detects pauses using a consensus observation across a configurable number of
 * detection thread. Detection threads can be set to periodically wakeup or continually spin.
 * <p>
 * All times and time units are in nanoseconds
 */
public class SimplePauseDetector extends PauseDetector {
    // All times and time units are in nanoseconds

    final static long DEFAULT_SleepInterval = 1000000L; // 1 msec
    final static long DEFAULT_PauseNotificationThreshold = 1000000L; // 1 msec
    final static int DEFAULT_NumberOfDetectorThreads = 3;
    final static boolean DEFAULT_Verbose = false;

    final long sleepInterval;
    final long pauseNotificationThreshold;
    final int numberOfDetectorThreads;
    boolean verbose;
    final AtomicLong consensusLatestTime = new AtomicLong();

    volatile long stallTheadMask = 0;
    volatile long stopThreadMask = 0;

    SimplePauseDetectorThread detectors[];

    /**
     * Creates a SimplePauseDetector
     * @param sleepInterval sleep interval used by detector threads
     * @param pauseNotificationThreshold minimum threshold for reporting detected pauses
     * @param numberOfDetectorThreads number of consensus detector threads to use
     */
    public SimplePauseDetector(long sleepInterval, long pauseNotificationThreshold,
                               int numberOfDetectorThreads) {
        this(sleepInterval, pauseNotificationThreshold, numberOfDetectorThreads, false);
    }

    /**
     * Creates a SimplePauseDetector
     * @param sleepInterval sleep interval used by detector threads
     * @param pauseNotificationThreshold minimum threshold for reporting detected pauses
     * @param numberOfDetectorThreads number of consensus detector threads to use
     * @param verbose provide verbose output when pauses are detected
     */
    public SimplePauseDetector(long sleepInterval, long pauseNotificationThreshold,
                               int numberOfDetectorThreads, boolean verbose) {
        this.sleepInterval = sleepInterval;
        this.pauseNotificationThreshold = pauseNotificationThreshold;
        this.numberOfDetectorThreads = numberOfDetectorThreads;
        this.verbose = verbose;
        detectors = new SimplePauseDetectorThread[numberOfDetectorThreads];
        for (int i = 0 ; i < numberOfDetectorThreads; i++) {
            detectors[i] = new SimplePauseDetectorThread(i);
            detectors[i].start();
        }
    }

    /**
     * Creates a SimplePauseDetector with a default sleep interval (1 msec), a default pause notification threshold
     * (1 msec), and using the default number of consensus detection threads (3).
     */
    public SimplePauseDetector() {
        this(DEFAULT_SleepInterval, DEFAULT_PauseNotificationThreshold,
                DEFAULT_NumberOfDetectorThreads, DEFAULT_Verbose);
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * Shut down the pause detector operation and terminate it's threads.
     */
    @Override
    public void shutdown() {
        stopThreadMask = 0xffffffffffffffffL;
        for (SimplePauseDetectorThread detector : detectors) {
            detector.interrupt();
        }
        super.shutdown();
    }

    class SimplePauseDetectorThread extends Thread {
        volatile long observedLasUpdateTime;
        final int threadNumber;
        final long threadMask;

        SimplePauseDetectorThread(final int threadNumber) {
            if ((threadNumber < 0) || (threadNumber > 63)) {
                throw new IllegalArgumentException("threadNumber must be between 0 and 63.");
            }
            this.threadNumber = threadNumber;
            this.threadMask = 1 << threadNumber;
            this.setDaemon(true);
            this.setName("SimplePauseDetectorThread_" + threadNumber);
        }

        public void run() {
            long shortestObservedTimeAroundLoop = Long.MAX_VALUE;

            observedLasUpdateTime = consensusLatestTime.get();
            long now = TimeServices.nanoTime();
            long prevNow = now;
            consensusLatestTime.compareAndSet(observedLasUpdateTime, now);

            while ((stopThreadMask & threadMask) == 0) {
                if (sleepInterval != 0) {
                    TimeServices.sleepNanos(sleepInterval);
                }

                // This is ***TEST FUNCTIONALITY***: Spin as long as we are externally asked to stall:
                while ((stallTheadMask & threadMask) != 0);

                observedLasUpdateTime = consensusLatestTime.get();
                // Volatile store above makes sure new "now" is measured after observedLasUpdateTime sample
                now = TimeServices.nanoTime();

                // Track shortest time around loop:
                shortestObservedTimeAroundLoop = Math.min(now - prevNow, shortestObservedTimeAroundLoop);

                // Update consensus time as long as it is is the past:
                while (now > observedLasUpdateTime) {
                    if (consensusLatestTime.compareAndSet(observedLasUpdateTime, now)) {
                        // Successfully and atomically moved consensus time forward. Act on the known delta:
                        final long deltaTimeNs = now - observedLasUpdateTime;

                        // Calculate hiccup time (accounting for known time around loop):
                        long hiccupTime = Math.max(deltaTimeNs - shortestObservedTimeAroundLoop, 0);

                        if (hiccupTime > pauseNotificationThreshold) {
                            if (verbose) {
                                System.out.println("SimplePauseDetector thread " + threadNumber +
                                        ": sending pause notification message: pause of " + hiccupTime +
                                        " nsec detected at nanoTime: " + now);
                            }
                            notifyListeners(hiccupTime, now);
                        }
                    } else {
                        // Failed to atomically move consensus time forward. Try again with current value:
                        observedLasUpdateTime = consensusLatestTime.get();
                    }
                }

                prevNow = now;
            }
            if (verbose) {
                System.out.println("SimplePauseDetector thread " + threadNumber + " terminating...");
            }
        }
    }


    /**
     * A test method that allows the caller to artificially stall a requested set of the detector threads for
     * a given amount of time. Used to verify pause detection when consensus occurs, as well as lack of detection
     * when it does not.
     * @param threadNumberMask a mask designating which threads should be stalled.
     * @param stallLength stall length, in nanosecond units
     */
    public void stallDetectorThreads(long threadNumberMask, long stallLength) throws InterruptedException {
        long savedMask = stallTheadMask;
        stallTheadMask = threadNumberMask;

        long startTime = TimeServices.nanoTime();
        long endTime = startTime + stallLength;
        for (long remainingTime = stallLength;
             remainingTime > 0;
             remainingTime = endTime - TimeServices.nanoTime()) {
            long timeDelta = Math.min(remainingTime, (pauseNotificationThreshold / 2));
            TimeServices.moveTimeForward(timeDelta);
            TimeUnit.NANOSECONDS.sleep(50000); // give things a chance to propagate.
        }

        stallTheadMask = savedMask;
    }

    /**
     * A test method that allows the caller to artificially move the consensus observed time forward
     * without causing a pause to be detected as a result of the time skip. Useful for test programs
     * that wish to use artificial time services.
     * @param newConsensusTime
     */
    public void skipConsensusTimeTo(long newConsensusTime) {
        consensusLatestTime.set(newConsensusTime);
    }
}
