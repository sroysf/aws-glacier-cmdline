package com.codechronicle.aws.glacier.event;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/24/12
 * Time: 4:15 PM
 * To change this template use File | Settings | File Templates.
 */
public interface EventListener {
    void onEvent(Event event);
}
