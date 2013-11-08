/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

public interface PauseDetectorListener {
    public void handlePauseEvent(long pauseLengthNsec, long pauseEndTimeNsec);
}
