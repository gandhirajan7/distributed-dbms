package com.dpgten.transactionprocessing;

import com.dpgten.constants.TransactionProcessingConstants;
import com.dpgten.logmanagement.LogWriterService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionDeleteQuery {

    Map<String, String> transactionMap = new HashMap<>();

    public Map deleteTransactionQuery(String queryString, Map dataMap, String selectedDatabase, String tableName) throws IOException {
        Map<Integer, String> currentDataMap = getTableRows(selectedDatabase, tableName);
        long startTime = System.nanoTime();
        if (queryString.split(" ").length < 4) {
            if (checkIfTableExists(selectedDatabase, tableName)) {
                dataMap = deleteAllRecordsInTable(tableName, dataMap, selectedDatabase);
            } else {
                transactionMap.put(TransactionProcessingConstants.CONCURRENT_TRANSACTION_LOG, "ERROR!!! Table does not exists");
                System.out.println("ERROR!!! Table does not exists");
            }
        } else {
            if (checkIfTableExists(selectedDatabase, tableName)) {
                dataMap = deleteRowsFromTable(queryString, tableName, selectedDatabase, currentDataMap);
            } else {
                transactionMap.put(TransactionProcessingConstants.CONCURRENT_TRANSACTION_LOG, "ERROR!!! Table does not exists");
                System.out.println("ERROR!!! Table does not exists");
            }
        }
        long endTime = System.nanoTime();
        transactionMap.put(TransactionProcessingConstants.QUERY_SUBMISSION_TIME, String.valueOf(endTime - startTime));
//        LogWriterService.getLogWriterServiceInstance().write(transactionMap);
        return dataMap;
    }

    private Map deleteRowsFromTable(String queryString, String tableName, String selectedDatabase, Map<Integer, String> currentDataMap) {
        Pattern pattern = Pattern.compile("delete from\\s+(.*)\\s+where\\s+(.*)");
        Matcher match = pattern.matcher(queryString);
        match.find();

        String whereColumnsNames = match.group(2).split("=")[0].replace(" ", "");
        String whereColumnValues = match.group(2).split("=")[1].replace(" ", "");
        int rowsAffected=0;
        List<String> columnsInTable = new ArrayList<>();
        List<String> columns = null;
        for (String line: currentDataMap.values()) {
            if (line.startsWith("Column")) {
                String[] columnsString = line.split(Pattern.quote("|"));
                columns = new LinkedList<>(Arrays.asList(columnsString));
                columns.remove(0);
                for (String columnType: columns) {
                    columnsInTable.add(columnType.split(",")[0]);
                }
            }
        }
        Map<String, String> linearMap = new HashMap<>();
        for (String line: currentDataMap.values()) {
            if (line.startsWith("Row")) {
                String[] rowString = line.split(Pattern.quote("|"));
                List<String> data = new LinkedList<>(Arrays.asList(rowString));
                data.remove(0);
                if (data.get(columnsInTable.indexOf(whereColumnsNames)).equals(whereColumnValues)) {
                    linearMap.put("delete_" + tableName + "_" + selectedDatabase, line);
                    rowsAffected = rowsAffected+1;
                }
            }
        }

        transactionMap.put(TransactionProcessingConstants.EXECUTED_QUERY_BY_USER, queryString);
        transactionMap.put(TransactionProcessingConstants.QUERY_EXECUTION_STATUS, "Success");
        transactionMap.put(TransactionProcessingConstants.DATABASE_CHANGE_LOG, "Rows Affected : " + rowsAffected);
        return linearMap;
    }

    private Map deleteAllRecordsInTable(String tableName, Map dataMap, String selectedDatabase) {
        dataMap.put("delete_"+tableName+"_"+ selectedDatabase, "deleteAllRecords");
        return dataMap;
    }

    private boolean checkIfTableExists(String selectedDatabase, String tableName) {
        File file = new File("src/main/esources/schema/" + selectedDatabase + "/" + tableName + ".txt");
        return file.exists();
    }

    public Map<Integer, String> getTableRows(String selectedDatabase, String tableName) throws IOException {
        Map<Integer, String> tableMap = new HashMap<>();
        String tableFile = "src/main/resources/schema/" + selectedDatabase + "/" + tableName + ".txt";
        BufferedReader reader = new BufferedReader(new FileReader(tableFile));
        String line;
        int lineCount = 1;
        while ((line = reader.readLine()) != null) {
            tableMap.put(lineCount, line);
            lineCount++;
        }
        return tableMap;
    }
}
