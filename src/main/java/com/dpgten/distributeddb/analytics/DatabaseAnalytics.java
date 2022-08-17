package com.dpgten.distributeddb.analytics;

public class DatabaseAnalytics {
    public static int DATABASE_QUERY_COUNT;
    public static int UPDATE_QUERY_COUNT;
    public static int CREATE_QUERY_COUNT;
    public static int INSERT_QUERY_COUNT;
    public static int SELECT_QUERY_COUNT;
    public static int TOTAL_QUERY_COUNT;


    public int getDATABASE_QUERY_COUNT() {
        return DATABASE_QUERY_COUNT;
    }

    public void setDATABASE_QUERY_COUNT(int DATABASE_QUERY_COUNT) {
        this.DATABASE_QUERY_COUNT = DATABASE_QUERY_COUNT;
    }

    public int getUPDATE_QUERY_COUNT() {
        return UPDATE_QUERY_COUNT;
    }

    public void setUPDATE_QUERY_COUNT(int UPDATE_QUERY_COUNT) {
        this.UPDATE_QUERY_COUNT = UPDATE_QUERY_COUNT;
    }

    public int getTOTAL_QUERY_COUNT() {
        return TOTAL_QUERY_COUNT;
    }

    public void setTOTAL_QUERY_COUNT(int TOTAL_QUERY_COUNT) {
        this.TOTAL_QUERY_COUNT = TOTAL_QUERY_COUNT;
    }

}

