package com.codechronicle.aws.glacier.event;

import org.testng.annotations.ExpectedExceptions;
import org.testng.annotations.Test;

import java.util.List;
import static org.testng.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/24/12
 * Time: 4:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventRegistryTest {

    @Test
    public void testDuplicates() {

        EventRegistry.clearAllListeners();

        EventListener listener = new EventListener() {
            @Override
            public void onEvent(Event event) {

            }
        };

        EventRegistry.register(EventType.UPLOAD_COMPLETE, listener);
        EventRegistry.register(EventType.UPLOAD_COMPLETE, listener);
        EventRegistry.register(EventType.UPLOAD_COMPLETE, listener);

        List<EventListener> eventListenerList = EventRegistry.getListeners(EventType.UPLOAD_COMPLETE);
        assertEquals(1, eventListenerList.size());

    }

    @Test
    public void testListeners() {
        EventRegistry.clearAllListeners();

        final StringBuilder message = new StringBuilder();

        EventListener listener = new EventListener() {
            @Override
            public void onEvent(Event event) {
                if (event.getEventType() == EventType.UPLOAD_COMPLETE) {
                    message.append("SUCCESS");
                }
            }
        };

        EventRegistry.register(EventType.UPLOAD_COMPLETE, listener);
        EventRegistry.publish(new Event(EventType.UPLOAD_COMPLETE, ""));
        assertEquals("SUCCESS", message.toString());
    }
}
