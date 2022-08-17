package com.dpgten.distributeddb.analytics;

public class AnalyticsOutput {
    public AnalyticsOutput(){
        System.out.println("user abc submitted "+  DatabaseAnalytics.TOTAL_QUERY_COUNT+ " queries for DB1 running on Virtual Machine 1");
        System.out.println("user abc updated "+  DatabaseAnalytics.UPDATE_QUERY_COUNT+ " queries for DB1 running on Virtual Machine 1");
        System.out.println("user abc create "+  DatabaseAnalytics.CREATE_QUERY_COUNT+ " queries for DB1 running on Virtual Machine 1");
        System.out.println("user abc select "+  DatabaseAnalytics.SELECT_QUERY_COUNT+ " queries for DB1 running on Virtual Machine 1");
        System.out.println("user abc insert "+  DatabaseAnalytics.INSERT_QUERY_COUNT+ " queries for DB1 running on Virtual Machine 1");
        System.out.println("user abc database "+  DatabaseAnalytics.DATABASE_QUERY_COUNT+ " queries for DB1 running on Virtual Machine 1");

    }
}
