package com.codechronicle.aws.glacier.event;

import org.codehaus.jackson.JsonNode;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/24/12
 * Time: 4:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class Event {
    private EventType eventType;
    private Object messagePayload;

    public Event(EventType eventType, Object messagePayload) {
        this.eventType = eventType;
        this.messagePayload = messagePayload;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public Object getMessagePayload() {
        return messagePayload;
    }

    public void setMessagePayload(Object messagePayload) {
        this.messagePayload = messagePayload;
    }
}
