package com.codechronicle.aws.glacier.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/24/12
 * Time: 4:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventRegistry {

    private static Map<EventType, List<EventListener>> registry = new HashMap<EventType, List<EventListener>>();

    public synchronized static void register(EventType type, EventListener listener) {
        List<EventListener> eventListeners = registry.get(type);
        if (eventListeners == null) {
            eventListeners = new ArrayList<EventListener>();
            registry.put(type, eventListeners);
        }

        if (!eventListeners.contains(listener)) {
            eventListeners.add(listener);
        }
    }

    public static List<EventListener> getListeners(EventType type) {
        return registry.get(type);
    }

    public synchronized static void deregister(EventType type, EventListener listener) {
        List<EventListener> eventListeners = registry.get(type);

        if (eventListeners != null) {
            eventListeners.remove(listener);
        }
    }

    public synchronized static void clearAllListeners() {
        registry.clear();
    }

    public synchronized static void publish (Event event) {
        List<EventListener> listeners = getListeners(event.getEventType());
        if (listeners != null) {
            for (EventListener listener : listeners) {
                listener.onEvent(event);
            }
        }
    }

}
