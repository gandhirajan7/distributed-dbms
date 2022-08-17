package com.dpgten.distributeddb.query;

import com.dpgten.distributeddb.access.RestCallController;
import com.dpgten.distributeddb.analytics.DatabaseAnalytics;
import com.dpgten.distributeddb.userauthentication.User;
import com.dpgten.distributeddb.utils.MetadataUtils;
import com.dpgten.transactionprocessing.TransactionQueryExecution;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Scanner;
import java.util.regex.Matcher;

import static com.dpgten.distributeddb.query.QueryParser.*;
import static com.dpgten.distributeddb.utils.Utils.*;

public class QueryImpl {
    DatabaseAnalytics databaseAnalytics = new DatabaseAnalytics();

    public boolean executeQuery(User user) {

//        String currentUser = user.getUsername();
        String currentDatabase = "";

        RestCallController restCallController = new RestCallController();
        String inputQuery = "1";

        DatabaseQuery dbQuery = new DatabaseQuery();
        while (inputQuery.toLowerCase(Locale.ROOT).equals("1")) {
            System.out.print("\nEnter Query Here ==>");
            Scanner input = new Scanner(System.in);
            QueryValidator validator = new QueryValidator();
            inputQuery = input.nextLine();
            if (validator.isUseQuery(inputQuery)) {
                currentDatabase = dbQuery.selectDatabase(inputQuery);
                if (currentDatabase.isEmpty()) {
                    System.out.println(RED + "DATABASE NOT FOUND" + RESET);
                } else {
                    System.out.println(YELLOW + "Database selected. Current Database is "
                            + BLUE + currentDatabase + YELLOW + "." + RESET);
                }
            } else if (currentDatabase.isEmpty()) {
                if (validator.isCreateQuery(inputQuery)) {
                    dbQuery.createDatabase(inputQuery);
                } else {
                    System.out.println(RED + "No Database selected, please select database!" + RESET);
                }
            } else if (validator.isCreateTableQuery(inputQuery)) {
                TableQuery tableQuery = new TableQuery();
                Matcher matcher = CREATE_TABLE_PATTERN.matcher(inputQuery);
                String tableName = "";
                if (matcher.find()) {
                    tableName = matcher.group(1);
                }
                tableQuery.createTable(inputQuery, currentDatabase);
            } else if (validator.isCreateQuery(inputQuery)) {
                dbQuery.createDatabase(inputQuery);
            } else if (validator.isSelectQuery(inputQuery)) {
                TableQuery tblQuery = new TableQuery();
                Matcher selectorMatcher = SELECT_TABLE_WHERE_PATTERN.matcher(inputQuery);
                if (selectorMatcher.find()) {
                    String tableName = selectorMatcher.group(8);
                    MetadataUtils mdUtils = new MetadataUtils();
                    String instance = mdUtils.getVMInstance(tableName);
                    String[] result = restCallController.selectRestCall(inputQuery, instance);
                    Arrays.asList(result).forEach(System.out::println);
                }
//                tblQuery.selectRows(inputQuery);
            } else if (validator.isInsertQuery(inputQuery)) {
                Matcher queryMatcher = INSERT_TABLE_PATTERN.matcher(inputQuery);
                String tableName = "";
                TableQuery tableQuery = new TableQuery();
                if (queryMatcher.find()) {
                    tableName = queryMatcher.group(1);
                    MetadataUtils mdUtils = new MetadataUtils();
                    String instance = mdUtils.getVMInstance(tableName);
                    boolean result = restCallController.insertRestCall(inputQuery, instance);
                    if (result) {
                        System.out.println("Inserted successfully");
                    }
                } else {
                    System.out.println("Insert failed error");
                }
//                tableQuery.insertRow(inputQuery);
            } else if (validator.isUpdateQuery(inputQuery)) {
                TableQuery tableQuery = new TableQuery();
                tableQuery.updateRow(inputQuery);
            } else if (dbQuery.isDeleteQuery(inputQuery)) {
                DeleteQueryParser deleteQueryParser = new DeleteQueryParser();
                deleteQueryParser.executeDeleteQueryWithConditionQuery(inputQuery, user);
            } else if (inputQuery.equalsIgnoreCase("BEGIN TRANSACTION") || inputQuery.equalsIgnoreCase("START TRANSACTION")) {
                TransactionQueryExecution transactionQueryExecution = new TransactionQueryExecution();
                try {
                    transactionQueryExecution.startTransaction(currentDatabase);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println(RED + "PLEASE ENTER VALID QUERY" + RESET + "\n");
            }
            System.out.println("please type " + RED + "1" + RESET + " to Continue");
            System.out.println("please type " + RED + "2" + RESET + " to Exit");
            System.out.print("Enter option here ==>");
            inputQuery = input.nextLine();
            DatabaseAnalytics.TOTAL_QUERY_COUNT++;
        }
        return true;
    }
}
