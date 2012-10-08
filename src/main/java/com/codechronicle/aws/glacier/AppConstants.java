package com.codechronicle.aws.glacier;

import java.text.SimpleDateFormat;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/23/12
 * Time: 5:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class AppConstants {

    private static final int ONE_MEGABYTE = 1024*1024;
    public static final int NETWORK_PARTITION_SIZE = ONE_MEGABYTE * 16;
    public static final String DATE_FORMAT = "EEE, d MMM yyyy hh:mm aaa";
}
