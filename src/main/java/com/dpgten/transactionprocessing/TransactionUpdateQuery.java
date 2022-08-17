package com.dpgten.transactionprocessing;

import com.dpgten.constants.TransactionProcessingConstants;
import com.fasterxml.jackson.databind.deser.DataFormatReaders;

import javax.swing.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 *
 * @author   Sai Vikas Chinthirla
 * Created   07 April, 2022
 * Version   1.0
 */


public class TransactionUpdateQuery {

    Map<String, String> transactionMap = new HashMap<>();

    public Map updateTransactionQuery(String query, Map dataMap, String selectedDatabase, String tableName) throws IOException {
        if (checkIfTableExists(selectedDatabase, tableName)) {
            Map<Integer, String> currentDataMap = getTableRows(selectedDatabase, tableName);
            String primaryKey = getPrimaryKeyFromCurrentFile(currentDataMap);
            if (checkIfPrimaryKeyPresent(query, primaryKey)) {
                if (checkIfDataTypeIsValid(query, tableName, currentDataMap, selectedDatabase)) {
                    String primaryKeyName = getPrimaryKey(currentDataMap);
                    int indexOfPrimaryKey = getIndexOfPrimaryKey(currentDataMap, primaryKeyName);
                    List<String> primaryKeyValuesList = getAllPrimaryKeyValues(currentDataMap, indexOfPrimaryKey);
                    if (!checkForPrimaryKeyDuplication(primaryKeyName, primaryKeyValuesList, query)) {
                        dataMap = updateRowsInTable(query, currentDataMap, tableName,selectedDatabase);
                    } else {
                        transactionMap.put(TransactionProcessingConstants.DATABASE_CRASH_REPORTS_LOG, "Primary Key cannot contains duplicate values!!!!");
                        System.out.println("Data Type Validation Failed");
                    }
                } else {
                    transactionMap.put(TransactionProcessingConstants.DATABASE_CRASH_REPORTS_LOG, "Data Type Validation Failed!!!!");
                    System.out.println("Data Type Validation Failed");
                }
            } else {
                dataMap = updateRowsInTable(query, currentDataMap, tableName,selectedDatabase);
            }
        } else {
            transactionMap.put(TransactionProcessingConstants.CONCURRENT_TRANSACTION_LOG, "Table does not exist");
            System.out.println("Table does not exist");
        }
        return dataMap;
    }

    private Map<String, String> updateRowsInTable(String query, Map<Integer, String> currentDataMap, String tableName, String selectedDatabase) {
        Pattern pattern = Pattern.compile("update\\s+(.*)\\s+set\\s+(.*)\\s+where\\s+(.*)");
        Matcher matcher = pattern.matcher(query);
        matcher.find();
        String[] queryUpdateColumns = matcher.group(2).split(",");
        List<String> queryColumnsList = Arrays.asList(queryUpdateColumns);
        Map<String, String> columnDataMap = new HashMap<>();
        List<String> queryColumns = new ArrayList<>();
        List<String> queryValues = new ArrayList<>();
        for (int i=0; i<queryColumnsList.size(); i++) {
            queryColumns.add(queryColumnsList.get(i).replaceAll("\\s", "").split("=")[0]);
            queryValues.add(queryColumnsList.get(i).replaceAll("\\s", "").split("=")[1]);
            columnDataMap.put(queryColumnsList.get(i).replaceAll("\\s", "").split("=")[0], queryColumnsList.get(i).replaceAll("\\s", "").split("=")[1]);
        }
        String whereClausePKValue = matcher.group(3).split("=")[1].replaceAll(";", "");
        String whereClausePKName = matcher.group(3).split("=")[0];
        Map<String, String> linearMap = new HashMap<>();
        //update users set userLName="vikas" where id=1
        // Row|1|sai|vikas|23
        List<String> columnsInTable = null;
        for (String line: currentDataMap.values()) {
            if (line.startsWith("Column")) {
                columnsInTable = new LinkedList<>(Arrays.asList(line.split(Pattern.quote("|"))));
                columnsInTable.remove(0);
                columnsInTable = columnsInTable.stream().map(s -> s.split(",")[0]).collect(Collectors.toList());
            }
        }
        int rowsAffected = 0;
        String updateRow = "Row|";
        for (String line: currentDataMap.values()) {
            if (line.startsWith("Row")) {
                boolean flag = Arrays.asList(line.split(Pattern.quote("|"))).get(columnsInTable.indexOf(whereClausePKName) + 1).equals(whereClausePKValue);
                String name = Arrays.asList(line.split(Pattern.quote("|"))).get(columnsInTable.indexOf(whereClausePKName) + 1);
                if (Arrays.asList(line.split(Pattern.quote("|"))).get(columnsInTable.indexOf(whereClausePKName) + 1).equals(whereClausePKValue)) {
                    for (int i=0; i<columnsInTable.size(); i++) {
                        if (queryColumns.contains(columnsInTable.get(i))) {
                            updateRow = updateRow + queryValues.get(queryColumns.indexOf(columnsInTable.get(i))) + "|";
                        } else {
                            updateRow = updateRow + line.split(Pattern.quote("|"))[i+1] + "|";
                        }
                    }
                    if (!line.equals(updateRow)) {
                        rowsAffected = rowsAffected+1;
                    }
                    linearMap.put("update_"+tableName +"_"+selectedDatabase, updateRow);
                }
            }
        }
        System.out.println("Rows Affected : " + rowsAffected);
        return linearMap;
    }

    private boolean checkForPrimaryKeyDuplication(String primaryKeyName, List<String> primaryKeyValuesList, String query) {
        Pattern pattern = Pattern.compile("update\\s+(.*)\\s+set\\s+(.*)\\s+where\\s+(.*)");
        Matcher matcher = pattern.matcher(query);
        matcher.find();
        List<String> columnNames = new ArrayList<>();
        List<String> columnValues = new ArrayList<>();
        String primaryKeyValue = null;
        String[] queryColumns = matcher.group(2).split(",");
        for (int i=0; i<queryColumns.length; i++) {
            if (queryColumns[i].replaceAll("\\s", "").split("=")[0].equals(primaryKeyName)) {
                primaryKeyValue = queryColumns[i].replaceAll("\\s", "").split("=")[1];
                break;
            }
        }
        if (primaryKeyValuesList.contains(primaryKeyValue))
            return true;
        return false;
    }

    private List<String> getAllPrimaryKeyValues(Map<Integer, String> currentDataMap, int indexOfPrimaryKey) {
        List<String> primaryKeyValues = new ArrayList<>();
        for (String lines: currentDataMap.values()) {
            if (lines.startsWith("Row")) {
                primaryKeyValues.add(lines.split(Pattern.quote("|"))[indexOfPrimaryKey]);
            }
        }
        return primaryKeyValues;
    }

    private int getIndexOfPrimaryKey(Map<Integer, String> currentDataMap, String primaryKeyName) {
        List<String> columnWithType = null;
        List<String> columnList = new ArrayList<>();
        for (String line: currentDataMap.values()) {
            if (line.contains("Column")) {
                String[] split = line.split(Pattern.quote("|"));
                columnWithType = new LinkedList<>(Arrays.asList(split));
                columnWithType.remove(0);
                for (String columnType: columnWithType) {
                    columnList.add(columnType.split(",")[0]);
                }
            }
        }
        System.out.println("primary Key index " + columnList.indexOf(primaryKeyName));
        return columnList.indexOf(primaryKeyName);
    }

    private String getPrimaryKey(Map<Integer, String> currentDataMap) {
        String primaryKey = null;
        for (String line: currentDataMap.values()) {
            if (line.contains("primary_key")) {
                primaryKey = line.split(Pattern.quote("|"))[1];
            }
        }
        return primaryKey;
    }

    private boolean checkIfDataTypeIsValid(String query, String tableName, Map<Integer, String> currentDataMap, String selectedDatabase) {
        List<String> dataTypeList = new ArrayList<>();
        List<String> tableColumnsList = new ArrayList<>();
        Pattern pattern = Pattern.compile("update\\s+(.*)\\s+set\\s+(.*)\\s+where\\s+(.*)");
        Matcher matcher = pattern.matcher(query);
        matcher.find();

        String[] newData = matcher.group(2).split(",");
        List<String> columnsInQuery = new ArrayList<>();
        List<String> valuesInQuery = new ArrayList<>();
        for (int i=0; i<newData.length; i++) {
            columnsInQuery.add(newData[i].split("=")[0]);
            valuesInQuery.add(newData[i].split("=")[0]);
        }

        for (String line: currentDataMap.values()) {
            if (line.startsWith("Column")) {
                String[] columnNamesString = line.split(Pattern.quote("|"));
                for (String s: columnNamesString) {
                    dataTypeList.add(s.split(",")[0]);
                    tableColumnsList.add(s.split(",")[1]);
                }
            }
        }
        boolean flag = false;
        for (int i=0; i<columnsInQuery.size(); i++) {
            String intVal = dataTypeList.get(tableColumnsList.indexOf(columnsInQuery.get(i)));
            if (intVal.equals("int") && !(valuesInQuery.get(i).matches("\\d"))) {
                flag = false;
            } else {
                flag = true;
            }
        }

        if (flag)
            return true;

        return false;
    }

    private boolean checkIfPrimaryKeyPresent(String query, String primaryKey) {
        Pattern pattern = Pattern.compile("update\\s+(.*)\\s+set\\s+(.*)\\s+where\\s+(.*)");
        Matcher matcher = pattern.matcher(query);
        matcher.find();
        String columnsString = matcher.group(2);
        String[] columns = columnsString.split(",");
        List<String> colList = new ArrayList<>();
        for (int i=0; i<columns.length; i++) {
            colList.add(columns[i].replaceAll("\\s", "").split("=")[0]);
        }
        return colList.contains(primaryKey);
    }

    private String getPrimaryKeyFromCurrentFile(Map<Integer, String> currentDataMap) {
        for (String line: currentDataMap.values()) {
            if (line.startsWith("primary_key")) {
                return line.split(Pattern.quote("|"))[1];
            }
        }
        return null;
    }

    private boolean checkIfTableExists(String selectedDatabase, String tableName) {
        File file = new File("src/main/resources/schema/" + selectedDatabase + "/" + tableName + ".txt");
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
