/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A PauseDetector detects pauses and reports them to registered listeners
 */
public abstract class PauseDetector extends Thread {

    ArrayList<PauseDetectorListener> listeners = new ArrayList<PauseDetectorListener>();
    LinkedBlockingQueue<Object> messages = new LinkedBlockingQueue<Object>();
    volatile boolean stop = false;

    PauseDetector() {
        this.start();
    }

    void notifyListeners(long pauseLengthNsec, long pauseEndTimeNsec) {
        ArrayList<PauseDetectorListener> listenersCopy =
                new ArrayList<PauseDetectorListener>(listeners.size());
        synchronized (this) {
            listenersCopy.addAll(listeners);
        }
        for (PauseDetectorListener listener : listenersCopy) {
            listener.handlePauseEvent(pauseLengthNsec, pauseEndTimeNsec);
        }
    }

    /**
     * Add a {@link org.LatencyUtils.PauseDetectorListener} listener to be notified when pauses are detected
     * @param listener Listener to add
     */
    public synchronized void addListener(PauseDetectorListener listener) {
        messages.add(new ChangeListenersRequest(ChangeListenersRequest.ChangeCommand.ADD, listener));
    }

    /**
     * Remove a {@link org.LatencyUtils.PauseDetectorListener}
     * @param listener Listener to remove
     */
    public synchronized void removeListener(PauseDetectorListener listener) {
        messages.add(new ChangeListenersRequest(ChangeListenersRequest.ChangeCommand.REMOVE, listener));
    }

    /**
     * Stop execution of this pause detector
     */
    public void shutdown() {
        stop = true;
        this.interrupt();
    }

    /**
     * @inheritDoc
     */
    public void run() {
        while (!stop) {
            try {
                Object message = messages.take();

                if (message instanceof ChangeListenersRequest) {
                    final ChangeListenersRequest changeRequest = (ChangeListenersRequest) message;
                    if (changeRequest.command == ChangeListenersRequest.ChangeCommand.ADD) {
                        listeners.add(changeRequest.listener);
                    } else {
                        listeners.remove(changeRequest.listener);
                    }

                } else if (message instanceof PauseNotification) {
                    final PauseNotification pauseNotification = (PauseNotification) message;

                    for (PauseDetectorListener listener : listeners) {
                        listener.handlePauseEvent(pauseNotification.pauseLengthNsec, pauseNotification.pauseEndTimeNsec);
                    }
                } else {
                    throw new RuntimeException("Unexpected message type received: " + message);
                }
            } catch (InterruptedException ex) {
            }
        }
    }

    static class ChangeListenersRequest {
        static enum ChangeCommand {ADD , REMOVE};

        final ChangeCommand command;
        final PauseDetectorListener listener;

        ChangeListenersRequest(final ChangeCommand changeCommand, final PauseDetectorListener listener) {
            this.command = changeCommand;
            this.listener = listener;
        }
    }

    static class PauseNotification {
        final long pauseLengthNsec;
        final long pauseEndTimeNsec;

        PauseNotification(final long pauseLengthNsec, final long pauseEndTimeNsec) {
            this.pauseLengthNsec = pauseLengthNsec;
            this.pauseEndTimeNsec = pauseEndTimeNsec;
        }
    }


}
