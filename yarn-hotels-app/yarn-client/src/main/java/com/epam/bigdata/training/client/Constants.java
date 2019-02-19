package com.epam.bigdata.training.client;

public class Constants {

    /**
     * Environment key name pointing to the the app master jar location
     */
    public static final String AM_JAR_PATH = "AM_JAR_PATH";

    /**
     * Environment key name pointing to the the app jar location
     */
    public static final String APP_JAR_PATH = "APP_JAR_PATH";

    /**
     * Environment key name denoting the file timestamp for the shell script.
     * Used to validate the local resource.
     */
    public static final String AM_JAR_TIMESTAMP = "AM_JAR_TIMESTAMP";

    /**
     * Environment key name denoting the file timestamp for the shell script.
     * Used to validate the local resource.
     */
    public static final String APP_JAR_TIMESTAMP = "APP_JAR_TIMESTAMP";

    /**
     * Environment key name denoting the file content length for the shell script.
     * Used to validate the local resource.
     */
    public static final String AM_JAR_LENGTH = "AM_JAR_LENGTH";

    /**
     * Environment key name denoting the file content length for the shell script.
     * Used to validate the local resource.
     */
    public static final String APP_JAR_LENGTH = "APP_JAR_LENGTH";


    public static final String AM_JAR_NAME = "AppMaster.jar";

    public static final String APP_JAR_NAME = "HotelsApplication.jar";


    public static final String APP_NAME = "APP_NAME";
    /**
     * Application main class to run in the container.
     * Contains actual business logic.
     */
    public static final String APP_MAIN_CLASS = "";

}
