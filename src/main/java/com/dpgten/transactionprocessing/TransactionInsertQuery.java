package com.dpgten.transactionprocessing;

import com.dpgten.constants.TransactionProcessingConstants;
import io.swagger.v3.oas.models.security.SecurityScheme;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author   Sai Vikas Chinthirla
 * Created   07 April, 2022
 * Version   1.0
 */

public class TransactionInsertQuery {

    Map<String,  String> transactionMap = new HashMap<>();


    public Map transactionInsertQuery(String queryString, Map dataMap, String selectedDatabase, String tableName) throws IOException {
        String[] querySplit = queryString.split(" ");
        if (checkIfTableExists(selectedDatabase, tableName)) {
            //parseQueryForValidation(queryString, dataMap, selectedDatabase, tableName)
            if (parseQueryForValidation(queryString, selectedDatabase, tableName)) {
                Map<Integer, String> currentData = getTableRows(selectedDatabase, tableName);
                String primaryKeyName = getPrimaryKey(currentData);
                if (primaryKeyName != null) {
                    int indexOfPrimaryKey = getIndexOfPrimaryKey(currentData, primaryKeyName);
                    List<String> primaryKeyValuesList = getAllPrimaryKeyValues(currentData, indexOfPrimaryKey);
                    if (!checkForDuplicatePrimaryKey(primaryKeyName, primaryKeyValuesList, queryString)) {
                        dataMap = insertData(tableName, selectedDatabase, queryString, currentData, dataMap);
                    } else {
                        transactionMap.put("Transactions", "Duplicate Primary Key Value Error!!!!!");
                        System.out.println("Duplicate Primary Key Value Error!!!!!");
                    }
                }
            } else {
                transactionMap.put("Transactions", "ERROR !!! Data for columns are incompatible.");
                System.out.println("Error in the written sql query related to data");
            }
        }
        return dataMap;
    }

    private Map insertData(String tableName, String selectedDatabase, String queryString, Map<Integer, String> currentData, Map queryDataMap) {
        Pattern pattern = Pattern.compile("insert into\\s+(.*?)\\s+\\((.*?)\\)\\s+values\\s+\\((.*?)\\)");
        Matcher matcher = pattern.matcher(queryString);
        matcher.find();
        String[] columnNames = matcher.group(2).replaceAll("\\s", "").split(",");
        String[] columnValues = matcher.group(3).split(",");
        List<String> columnNamesList = Arrays.asList(columnNames);
        Map<String, String> columnDataMap = new HashMap<>();
        for (int i=0; i<columnNamesList.size(); i++) {
            columnDataMap.put(columnNames[i], columnValues[i]);
        }
        List<String> colList = null;
        for (String line: currentData.values()) {
            if (line.startsWith("Column")) {
                colList = new LinkedList<>(Arrays.asList(line.split(Pattern.quote("|"))));
            }
        }
        colList.remove(0);
        Integer lineNo = 0;
        for (int lineNumber: currentData.keySet()) {
            lineNo = lineNumber;
        }
        String insertString = "Row|";
        for (int i=0; i<colList.size(); i++) {
            insertString = insertString + columnDataMap.get(columnNamesList.get(i)).trim();
            int delimeter = i;
            if (++delimeter < colList.size())
                insertString = insertString + "|";
        }
        currentData.put(lineNo+1, insertString);
        queryDataMap.put("insert_"+tableName+"_"+selectedDatabase, insertString);
        return queryDataMap;
    }

    private boolean checkForDuplicatePrimaryKey(String primaryKey, List<String> primaryKeyValuesList, String queryString) {
        Pattern pattern = Pattern.compile("insert into\\s+(.*?)\\s+\\((.*?)\\)\\s+values\\s+\\((.*?)\\)");
        Matcher matcher = pattern.matcher(queryString);
        matcher.find();
        String[] columnNames = matcher.group(2).replaceAll("\\s", "").split(",");
        String[] columnValues = matcher.group(3).split(",");
        if (columnNames.length != columnValues.length) {
            transactionMap.put(TransactionProcessingConstants.DATABASE_CRASH_REPORTS_LOG, "ERROR!!! Number of columns and values must be same");
            System.out.println("ERROR!!! Number of columns and values must be same");
        }
        int primaryKeyIndex = Arrays.asList(columnNames).indexOf(primaryKey);
        if (primaryKeyIndex < 0) {
            transactionMap.put(TransactionProcessingConstants.DATABASE_CRASH_REPORTS_LOG, "ERROR!!! Primary Key value cannot be empty!");
            System.out.println("ERROR!!! Primary Key value cannot be empty!");
        }
        List<String> columnValList = Arrays.asList(columnValues);
        if (primaryKeyValuesList.contains(columnValList.get(primaryKeyIndex))) {
            return true;
        }
        return false;
    }

    private List<String> getAllPrimaryKeyValues(Map<Integer, String> dataMap, int indexOfPrimaryKey) {
        List<String> primaryKeyValues = new ArrayList<>();
        for (String lines: dataMap.values()) {
            if (lines.startsWith("Row")) {
                primaryKeyValues.add(lines.split(Pattern.quote("|"))[indexOfPrimaryKey+1]);
            }
        }
        return primaryKeyValues;
    }

    private int getIndexOfPrimaryKey(Map<Integer, String> dataMap, String primaryKey) {
        List<String> columnWithType = null;
        List<String> columnList = new ArrayList<>();
        for (String line: dataMap.values()) {
            if (line.contains("Column")) {
                String[] split = line.split(Pattern.quote("|"));
                columnWithType = new LinkedList<>(Arrays.asList(split));
                columnWithType.remove(0);
                for (String columnType: columnWithType) {
                    columnList.add(columnType.split(",")[0]);
                }
            }
        }
        System.out.println("primary Key index " + columnList.indexOf(primaryKey));
        return columnList.indexOf(primaryKey);
    }

    private String getPrimaryKey(Map<Integer, String> dataMap) {
        String primaryKey = null;
        for (String line: dataMap.values()) {
            if (line.contains("primary_key")) {
                primaryKey = line.split(Pattern.quote("|"))[1];
            }
        }
        return primaryKey;
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

    private boolean parseQueryForValidation(String queryString, String selectedDatabase, String tableName) throws IOException {
        Map<Integer, String> dataMap = getTableRows(selectedDatabase, tableName);
        List<String> tableColumns = new ArrayList<>();
        //((?<=(INSERT\\sINTO\\s))[\\w\\d_]+(?=\\s+))|((?<=\\()([\\w\\d_,]+)+(?=\\)))
        Pattern pattern = Pattern.compile("insert into\\s+(.*?)\\s+\\((.*?)\\)\\s+values\\s+\\((.*?)\\)");
        Matcher matcher = pattern.matcher(queryString);
        matcher.find();
        List<String> dataTypes = null;
        List<String> columnNames = new ArrayList<>();
        boolean validationFlag = false;
        String[] queryColumnNames = matcher.group(2).replaceAll("\\s", "").split(",");
        String[] columnValues = matcher.group(3).split(",");
        tableColumns = new LinkedList<String>(Arrays.asList(queryColumnNames));
        for (String line: dataMap.values()) {
            if (line.startsWith("Column")) {
                String[] splitNames = line.split(Pattern.quote("|"));
                dataTypes = new LinkedList<>(Arrays.asList(splitNames));
                dataTypes.remove(0);
                List<String> finalDataTypes = new ArrayList<>();
                for (String dataType: dataTypes) {
                    String val = dataType.split(",")[1];
                    columnNames.add(dataType.split(",")[0]);
                    finalDataTypes.add(val);
                }
                dataTypes = finalDataTypes;
            }
        }

        for (int i=0; i<tableColumns.size(); i++) {
            String val = dataTypes.get(columnNames.indexOf(tableColumns.get(i)));
            if (!(columnValues[i].matches("\\d+")) && val.equalsIgnoreCase("int"))
                validationFlag = false;
            else
                validationFlag  = true;
        }

        if (validationFlag)
            return true;

        return validationFlag;
    }

    private boolean checkIfTableExists(String selectedDatabase, String tableName) {
        File file = new File("src/main/resources/schema/" + selectedDatabase + "/" + tableName + ".txt");
        return file.exists();
    }
}
