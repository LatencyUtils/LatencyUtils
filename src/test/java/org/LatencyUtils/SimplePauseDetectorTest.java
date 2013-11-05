/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.junit.*;
import java.io.*;
import java.lang.ref.WeakReference;


/**
 * JUnit test for {@link SimplePauseDetector}
 */
public class SimplePauseDetectorTest {

    static long detectedPauseLength = 0;

    @Test
    public void testSimpleSleepingPauseDetectorDetects() throws Exception {
        SimplePauseDetector pauseDetector = new SimplePauseDetector(1000000L /* 1 msec sleep */,
                10000000L /* 10msec reporting threashold */, 3 /* thread count */);

        PauseTracker tracker = new PauseTracker(pauseDetector, this);
        try {
            detectedPauseLength = 0;

            Thread.sleep(50);

            System.out.println("trying to stall sleeping thread 0 for 20msec:");

            pauseDetector.stallDetectorThreads(0x1, 20000000L);

            System.out.println("trying to stall sleeping thread 1 for 20msec:");

            pauseDetector.stallDetectorThreads(0x2, 20000000L);

            System.out.println("trying to stall sleeping thread 2 for 20msec:");

            pauseDetector.stallDetectorThreads(0x4, 20000000L);

            Assert.assertTrue("detected pause needs to be 0 msec, but was " + detectedPauseLength/1000000.0 + " msec instead.",
                    detectedPauseLength == 0L);

            System.out.println("trying to stall all 3 sleeping threads for 20msec:");

            pauseDetector.stallDetectorThreads(0x7, 20000000L);

            Thread.sleep(500);

            Assert.assertTrue("detected pause needs to be at least 10 msec, but was " + detectedPauseLength/1000000.0 + " msec instead.",
                    detectedPauseLength > 10000000L);
        } catch (InterruptedException ex) {

        }
        pauseDetector.shutdown();
    }

    @Test
    public void testSimpleSpinningPauseDetectorDetects() throws Exception {
        SimplePauseDetector pauseDetector = new SimplePauseDetector(0 /* 0 msec sleep */,
                50000L /* 10msec reporting threashold */, 3 /* thread count */);

        PauseTracker tracker = new PauseTracker(pauseDetector, this);
        try {
            detectedPauseLength = 0;

            Thread.sleep(50);

            Assert.assertTrue("detected pause needs to be 0 msec, but was " + detectedPauseLength/1000000.0 + " msec instead.",
                    detectedPauseLength == 0L);

            System.out.println("trying to stall all 3 spinning threads for 80 usec:");

            pauseDetector.stallDetectorThreads(0x7, 80000L);

            Thread.sleep(500);

            Assert.assertTrue("detected pause needs to be at least 10 msec, but was " + detectedPauseLength/1000000.0 + " msec instead.",
                    detectedPauseLength > 50000L);
        } catch (InterruptedException ex) {

        }
        pauseDetector.shutdown();
    }

    static class PauseTracker extends WeakReference<SimplePauseDetectorTest> implements PauseDetector.PauseDetectorListener {
        final PauseDetector pauseDetector;

        PauseTracker(final PauseDetector pauseDetector, final SimplePauseDetectorTest test) {
            super(test);
            this.pauseDetector = pauseDetector;
            // Register as listener:
            pauseDetector.addListener(this);
        }

        public void handlePauseEvent(final long pauseLengthNsec, final long pauseEndTimeNsec) {
            final SimplePauseDetectorTest test = this.get();

            if (test != null) {
                System.out.println("Pause detected: paused for " + pauseLengthNsec + " nsec, at " + pauseEndTimeNsec);
                detectedPauseLength = pauseLengthNsec;
            } else {
                // Remove listener:
                pauseDetector.removeListener(this);
            }
        }
    }
}

