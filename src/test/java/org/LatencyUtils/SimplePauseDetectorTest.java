/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.junit.*;
import java.io.*;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicLong;


/**
 * JUnit test for {@link SimplePauseDetector}
 */
public class SimplePauseDetectorTest {

    @Test
    public void testSimpleSleepingPauseDetectorDetects() throws Exception {
        AtomicLong detectedPauseLength = new AtomicLong(0);

        SimplePauseDetector pauseDetector = new SimplePauseDetector(1000000L /* 1 msec sleep */,
                10000000L /* 10 msec reporting threshold */, 3 /* thread count */, true /* verbose */);

        System.out.println("Starting 1 msec, 3 thread sleeping pause detector:");

        PauseTracker tracker = new PauseTracker(pauseDetector, this, detectedPauseLength);
        try {
            Thread.sleep(100);

            detectedPauseLength.set(0);

            Thread.sleep(100);

            System.out.println("trying to stall sleeping thread 0 for 20msec:");

            pauseDetector.stallDetectorThreads(0x1, 20000000L);

            System.out.println("trying to stall sleeping thread 1 for 20msec:");

            pauseDetector.stallDetectorThreads(0x2, 20000000L);

            System.out.println("trying to stall sleeping thread 2 for 20msec:");

            pauseDetector.stallDetectorThreads(0x4, 20000000L);

            Assert.assertTrue("detected pause needs to be 0 msec, but was " + detectedPauseLength.get()/1000000.0 + " msec instead.",
                    detectedPauseLength.get() == 0L);

            System.out.println("trying to stall all 3 sleeping threads for 20msec:");

            detectedPauseLength.set(0);

            pauseDetector.stallDetectorThreads(0x7, 20000000L);

            Thread.sleep(500);

            Assert.assertTrue("detected pause needs to be at least 10 msec, but was " + detectedPauseLength.get()/1000000.0 + " msec instead.",
                    detectedPauseLength.get() > 10000000L);
        } catch (InterruptedException ex) {

        }

        tracker.stop();
        pauseDetector.shutdown();
    }

    @Test
    public void testSimpleShortSleepingPauseDetectorDetects() throws Exception {
        AtomicLong detectedPauseLength = new AtomicLong(0);

        SimplePauseDetector pauseDetector = new SimplePauseDetector(20000L /* 20 usec sleep */,
                2000000L /* 2 msec reporting threshold */, 3 /* thread count */, true /* verbose */);

        System.out.println("Starting 250 usec, 3 thread sleeping pause detector:");

        PauseTracker tracker = new PauseTracker(pauseDetector, this, detectedPauseLength);
        try {

            Thread.sleep(100);

            detectedPauseLength.set(0);

            Thread.sleep(2000);

            Assert.assertTrue("detected pause needs to be 0 msec, but was " + detectedPauseLength.get()/1000000.0 + " msec instead.",
                    detectedPauseLength.get() == 0L);

            System.out.println("trying to stall all 3 sleeping threads for 300usec:");

            detectedPauseLength.set(0);

            pauseDetector.stallDetectorThreads(0xffff, 3000000L);

            Thread.sleep(500);

            Assert.assertTrue("detected pause needs to be at least 2000 usec, but was " + detectedPauseLength.get()/1000000.0 + " msec instead.",
                    detectedPauseLength.get() > 2000000L);

        } catch (InterruptedException ex) {

        }

        tracker.stop();
        pauseDetector.shutdown();
    }

    @Test
    public void testSimpleSpinningPauseDetectorDetects() throws Exception {
        AtomicLong detectedPauseLength = new AtomicLong(0);

        SimplePauseDetector pauseDetector = new SimplePauseDetector(0 /* 0 msec sleep */,
                50000L /* 250 usec reporting threshold */, 3 /* thread count */, true /* verbose */);

        System.out.println("Starting 50 usec, 3 thread spinning pause detector:");

        PauseTracker tracker = new PauseTracker(pauseDetector, this, detectedPauseLength);
        try {
            Thread.sleep(1000);

            detectedPauseLength.set(0);

            Thread.sleep(100);

            Assert.assertTrue("detected pause needs to be 0 msec, but was " + detectedPauseLength.get()/1000000.0 + " msec instead.",
                    detectedPauseLength.get() == 0L);

            System.out.println("trying to stall all 3 spinning threads for 100 usec:");

            detectedPauseLength.set(0);

            pauseDetector.stallDetectorThreads(0x7, 100000L);

            Thread.sleep(500);

            Assert.assertTrue("detected pause needs to be at least 50 usec, but was " + detectedPauseLength.get()/1000000.0 + " msec instead.",
                    detectedPauseLength.get() > 50000L);
        } catch (InterruptedException ex) {

        }

        tracker.stop();
        pauseDetector.shutdown();
    }

    static class PauseTracker extends WeakReference<SimplePauseDetectorTest> implements PauseDetectorListener {
        final PauseDetector pauseDetector;
        final AtomicLong detectedPauseLength;

        PauseTracker(final PauseDetector pauseDetector, final SimplePauseDetectorTest test, AtomicLong detectedPauseLength) {
            super(test);
            this.pauseDetector = pauseDetector;
            this.detectedPauseLength = detectedPauseLength;
            // Register as listener:
            pauseDetector.addListener(this);
        }

        public void stop() {
            pauseDetector.removeListener(this);
        }

        public void handlePauseEvent(final long pauseLengthNsec, final long pauseEndTimeNsec) {
            final SimplePauseDetectorTest test = this.get();

            if (test != null) {
                System.out.println("Pause detected: paused for " + pauseLengthNsec + " nsec, at " + pauseEndTimeNsec);
                detectedPauseLength.set(pauseLengthNsec);
            } else {
                // Remove listener:
                pauseDetector.removeListener(this);
            }
        }
    }
}

