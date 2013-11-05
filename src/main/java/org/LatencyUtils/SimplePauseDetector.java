/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.util.concurrent.atomic.AtomicLong;

public class SimplePauseDetector extends PauseDetector {
    final static long DEFAULT_SleepIntervalNsec = 1000000L; // 1 msec
    final static long DEFAULT_PauseNotificationThresholdNsec = 10000000L; // 10 msec
    final static int DEFAULT_NumberOfDetectorThreads = 3;
    final static boolean DEFAULT_Verbose = false;

    final long sleepIntervalNsec;
    final long pauseNotificationThresholdNsec;
    final int numberOfDetectorThreads;
    final boolean verbose;
    final AtomicLong consensusLatestTime = new AtomicLong();

    volatile long stallTheadMask = 0;
    volatile long stopThreadMask = 0;

    SimplePauseDetectorThread detectors[];

    public SimplePauseDetector(long sleepIntervalNsec, long pauseNotificationThresholdNsec,
                               int numberOfDetectorThreads) {
        this(sleepIntervalNsec, pauseNotificationThresholdNsec, numberOfDetectorThreads, false);
    }

    public SimplePauseDetector(long sleepIntervalNsec, long pauseNotificationThresholdNsec,
                               int numberOfDetectorThreads, boolean verbose) {
        this.sleepIntervalNsec = sleepIntervalNsec;
        this.pauseNotificationThresholdNsec = pauseNotificationThresholdNsec;
        this.numberOfDetectorThreads = numberOfDetectorThreads;
        this.verbose = verbose;
        detectors = new SimplePauseDetectorThread[numberOfDetectorThreads];
        for (int i = 0 ; i < numberOfDetectorThreads; i++) {
            detectors[i] = new SimplePauseDetectorThread(i);
            detectors[i].start();
        }
    }

    public SimplePauseDetector() {
        this(DEFAULT_SleepIntervalNsec, DEFAULT_PauseNotificationThresholdNsec,
                DEFAULT_NumberOfDetectorThreads, DEFAULT_Verbose);
    }

    @Override
    public void shutdown() {
        stopThreadMask = 0xffffffffffffffffL;
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
            final long sleepIntervalMsec = (sleepIntervalNsec + (1000000L - 1)) / 1000000L; // round up to msec.
            observedLasUpdateTime = consensusLatestTime.get();
            long now = System.nanoTime();
            long prevNow = now;
            consensusLatestTime.compareAndSet(observedLasUpdateTime, now);

            long shortestObservedTimeAroundLoop = Long.MAX_VALUE;
            try {
                while ((stopThreadMask & threadMask) == 0) {
                    if (sleepIntervalNsec != 0) {
                        Thread.sleep(sleepIntervalMsec);
                    }

                    // TEST FUNCIONALITY: Spin as long as we are asked to stall:
                    while ((stallTheadMask & threadMask) != 0);

                    observedLasUpdateTime = consensusLatestTime.get();
                    // Volatile store above makes sure new now is measured after observedLasUpdateTime sample
                    now = System.nanoTime();
                    // Move consensus forward and act on delta:
                    if (consensusLatestTime.compareAndSet(observedLasUpdateTime, now)) {
                        final long deltaTimeNs = now - observedLasUpdateTime;

                        final long timeAroundLoop = now - prevNow;
                        if (timeAroundLoop < shortestObservedTimeAroundLoop) {
                            shortestObservedTimeAroundLoop = timeAroundLoop;
                        }

                        long hiccupTimeNsec = deltaTimeNs - shortestObservedTimeAroundLoop;
                        hiccupTimeNsec = (hiccupTimeNsec < 0) ? 0 : hiccupTimeNsec;

                        if (hiccupTimeNsec > pauseNotificationThresholdNsec) {
                            System.out.println("thread " + threadNumber + ": sending pause notification message.");
                            messages.add(new PauseNotification(hiccupTimeNsec, now));
                        }
                    }
                    prevNow = now;
                }
            } catch (InterruptedException e) {
                if (verbose) System.err.println("SimplePauseDetectorThread interrupted/terminating...");
            }
        }
    }


    /**
     * A test method that allows the caller to artificially stall a requested set of the detector threads for
     * a given amount of time. Used to verify pause detection when consensus occurs, as well as lack of detection
     * when it does not.
     * @param threadNumberMask
     * @param stallLengthNsec
     */
    public void stallDetectorThreads(long threadNumberMask, long stallLengthNsec) {
        long savedMask = stallTheadMask;
        stallTheadMask = threadNumberMask;
        try {
            if (stallLengthNsec > 1000000L) {
                Thread.sleep(stallLengthNsec / 1000000L);
            } else {
                long startTime = System.nanoTime();
                while (System.nanoTime() < (stallLengthNsec + startTime));
            }
        } catch (InterruptedException ex) {
        }
        stallTheadMask = savedMask;
    }
}
