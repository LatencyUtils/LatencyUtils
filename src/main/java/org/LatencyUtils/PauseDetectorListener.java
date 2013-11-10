/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

/**
 * Accepts pause notification events.
 * <p>
 * All times and time units are in nanoseconds
 */
public interface PauseDetectorListener {

    /**
     * handle a pause event notification.
     * @param pauseLength Length of reported pause (in nanoseconds)
     * @param pauseEndTime Time sampled at end of reported pause (in nanoTime units).
     */
    public void handlePauseEvent(long pauseLength, long pauseEndTime);
}
