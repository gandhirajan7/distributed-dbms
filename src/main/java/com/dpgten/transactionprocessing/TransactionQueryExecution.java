package com.dpgten.transactionprocessing;

import com.dpgten.constants.TransactionProcessingConstants;
import com.dpgten.logmanagement.LogWriterService;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import static com.dpgten.distributeddb.utils.Utils.RESET;
import static com.dpgten.distributeddb.utils.Utils.YELLOW;

/**
 *
 * @author   Sai Vikas Chinthirla
 * Created   07 April, 2022
 * Version   1.0
 */

public class TransactionQueryExecution {

    public Map<String, String> transactionMap = new HashMap<>();
    public List<String> queriesList = new ArrayList<>();
    Map dataMap = new HashMap();

    public void startTransaction(String selectedDatabase) throws IOException {
        System.out.println("Starting Transaction !!");
        Scanner queryScanner = new Scanner(System.in);
        while (true) {
            System.out.println("Enter the query or end transaction (commit) : ");
            String query = queryScanner.nextLine();
            queriesList.add(query);
            if (query.toLowerCase().contains("end"))
                break;
            else if (query.toLowerCase().contains("commit") || query.toLowerCase().contains("rollback")) {
                break;
            }
        }
        long startTime = System.nanoTime();
        for (String queryString: queriesList) {
            String[] querySplit = queryString.split(" ");
            String tableName = null;
//            System.out.println(Arrays.toString(querySplit));
            if (queryString.toLowerCase().contains("select")) {
                tableName = querySplit[3];
                dataMap = checkTableLocking(queryString, tableName, selectedDatabase, dataMap);
                createTempTable(dataMap, tableName, selectedDatabase);
                releaseLock(tableName);
            } else if (queryString.toLowerCase().contains("insert")) {
                System.out.println("Inside insert for executing");
                tableName = querySplit[2];
                System.out.println(tableName);
                dataMap = checkTableLocking(queryString, tableName, selectedDatabase, dataMap);
                releaseLock(tableName);
            } else if (queryString.toLowerCase().contains("update")) {
                tableName = querySplit[1];
                dataMap = checkTableLocking(queryString, tableName, selectedDatabase, dataMap);
                releaseLock(tableName);
            } else if (queryString.toLowerCase().contains("delete")) {
                tableName = querySplit[2];
                dataMap = checkTableLocking(queryString, tableName, selectedDatabase, dataMap);
                releaseLock(tableName);
            } else if (queryString.toLowerCase().contains("commit")) {
                commitAllTheChanges(dataMap);
            } else if (queryString.toLowerCase().contains("rollback")) {
                rollBackAllTheChanges(dataMap);
            }
        }
        long endTime = System.nanoTime();
        transactionMap.put(TransactionProcessingConstants.QUERY_EXECUTION_TIME, "Total query execution Time : " + (endTime - startTime));
        transactionMap.put(TransactionProcessingConstants.CONCURRENT_TRANSACTION_LOG, "Total Transaction Execution Time : " + (endTime - startTime));
        LogWriterService.getLogWriterServiceInstance().write(transactionMap);
    }

    private void rollBackAllTheChanges(Map dataMap) {
        dataMap.clear();
        transactionMap.put(TransactionProcessingConstants.CONCURRENT_TRANSACTION_LOG, "All the changes are rollbacked as rollback command was given");
        System.out.println("Roll backed all the changes");
    }

    private void commitAllTheChanges(Map<String, String> dataMap) throws IOException {
        for (Map.Entry<String, String> entry: dataMap.entrySet()) {
            if (entry.getKey().contains("insert")) {
                commitInsertRelatedChanges(entry.getKey(), entry.getValue());
            } else if (entry.getKey().contains("update")) {
                commitUpdateRelatedChanges(entry.getKey(), entry.getValue());
            } else if (entry.getKey().contains("delete")) {
                commitDeleteRelatedChanges(entry.getKey(), entry.getValue());
            }
        }
    }

    private void commitDeleteRelatedChanges(String key, String value) throws IOException {
        String tableName = key.split("_")[1];
        String databaseName = key.split("_")[2];
        String tableLocation = "src/main/resources/schema/" + databaseName +"/" + tableName + ".txt";
        Map<Integer, String> currentDataMap = getTableRows(databaseName, tableName);
        String newline = System.getProperty("line.separator");
        if (value.contains("deleteAllRecords")) {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tableLocation, false));
            for (String line: currentDataMap.values()) {
                if (!line.startsWith("Row")) {
                    bufferedWriter.append(line + newline);
                }
            }
            bufferedWriter.flush();
        } else {
            BufferedWriter singleRowDelete = new BufferedWriter(new FileWriter(tableLocation, false));
            String primaryKeyValue = Arrays.asList(value.split(Pattern.quote("|"))).get(1);
            for (String line: currentDataMap.values()) {
                if (line.startsWith("Row")) {
                    if (!Arrays.asList(line.split(Pattern.quote("|"))).get(1).equals(primaryKeyValue)) {
                        singleRowDelete.append(line + newline);
                    }
                } else {
                    singleRowDelete.append(line + newline);
                }
            }
            singleRowDelete.flush();
        }
    }

    private void commitUpdateRelatedChanges(String key, String value) throws IOException {
        String tableName = key.split("_")[1];
        String databaseName = key.split("_")[2];
        String tableLocation = "src/main/resources/schema/" + databaseName +"/" + tableName + ".txt";
        String[] primaryKeySplit = value.split(Pattern.quote("|"));
        List<String> primaryKeyList = Arrays.asList(primaryKeySplit);
        String primaryKey = primaryKeyList.get(1);
        Map<Integer, String> currentDateMap = getTableRows(databaseName, tableName);
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tableLocation, false));
        String newline = System.getProperty("line.separator");
        for (String line: currentDateMap.values()) {
            if (line.startsWith("Row") && line.split(Pattern.quote("|"))[1].equals(primaryKey)) {
                bufferedWriter.append(value + newline);
            } else {
                bufferedWriter.append(line + newline);
            }
        }
        bufferedWriter.flush();
    }

    private void commitInsertRelatedChanges(String key, String value) {
        String tableName = key.split("_")[1];
        String databaseName = key.split("_")[2];
        String newline = System.getProperty("line.separator");
        String tableLocation = "src/main/resources/schema/" + databaseName +"/" + tableName + ".txt";
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tableLocation, true));
//            bufferedWriter.newLine();
            bufferedWriter.append(newline + value);
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        transactionMap.put(TransactionProcessingConstants.EXECUTED_QUERY_BY_USER, "inserted the date");
        System.out.println(YELLOW +"1 row affected" + RESET);
    }

    public Map checkTableLocking(String queryString, String tableName, String selectedDatabase, Map dataMap) throws IOException {

        if (!checkIfTableIsLocked(tableName)) {
            acquireLocks(tableName);
            transactionMap.put(TransactionProcessingConstants.EXECUTED_QUERY_BY_USER, queryString);
            transactionMap.put(TransactionProcessingConstants.CONCURRENT_TRANSACTION_LOG, "LOCK applied on table " + tableName);
//            dataMap = getTableRows(selectedDatabase, tableName);
            return processQuery(queryString, dataMap, selectedDatabase, tableName);

        } else {
            transactionMap.put(TransactionProcessingConstants.CONCURRENT_TRANSACTION_LOG, "ERROR !!! Table is currently locked" + tableName);
            System.out.println("ERROR !!! Table is currently locked" + tableName);
        }
        return null;
    }

    private Map processQuery(String queryString, Map dataMap, String selectedDatabase, String tableName) throws IOException {
        String queryType = queryString.split(" ")[0];
        if (queryType.equalsIgnoreCase("insert")) {
            TransactionInsertQuery insertQuery = new TransactionInsertQuery();
            try {
                return insertQuery.transactionInsertQuery(queryString, dataMap, selectedDatabase, tableName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (queryType.equalsIgnoreCase("update")) {
            TransactionUpdateQuery transactionUpdateQuery = new TransactionUpdateQuery();
            return transactionUpdateQuery.updateTransactionQuery(queryString, dataMap, selectedDatabase, tableName);
        } else if (queryType.equalsIgnoreCase("delete")) {
            TransactionDeleteQuery transactionDeleteQuery = new TransactionDeleteQuery();
            return transactionDeleteQuery.deleteTransactionQuery(queryString, dataMap, selectedDatabase, tableName);
        }
        return null;
    }

    public boolean checkIfTableIsLocked(String tableName) throws IOException {
        String lockMangerLocation = "src/main/resources/schema/LockManager.txt";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(TransactionProcessingConstants.LOCK_MANAGER_FILE_LOCATION));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            if (line.equals(tableName))
                return true;
        }
        return false;
    }

    public void acquireLocks(String tableName) {
        String separator = System.getProperty("line.separator");
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(TransactionProcessingConstants.LOCK_MANAGER_FILE_LOCATION, true));
            writer.append(tableName + separator);
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void createTempTable(Map<Integer, String> dataMap, String tableName, String selectedDatabase) {
        String tempFilePath = "src/main/resources/schema/" + selectedDatabase + "/" + tableName + ".txt";
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFilePath));
            dataMap.keySet().forEach(key -> {
                try {
                    writer.append(dataMap.get(key));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void releaseLock(String tableName) {
        String lockManagerLocation = "src/main/resources/schema/LockManager.txt";
        try {
            BufferedReader br = new BufferedReader(new FileReader(lockManagerLocation));
            String line = null;
            int lineNo = 0;
            Map<Integer, String> locks = new HashMap<>();
            while ((line = br.readLine()) != null) {
                if(!line.equals(tableName)) {
                    locks.put(lineNo, line);
                    lineNo += 1;
                }
            }
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(lockManagerLocation, false));
            for (Object val: locks.values()) {
                fileWriter.write(val.toString() + System.getProperty("line.separator"));
            }
            fileWriter.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<Integer, String> getTableRows(String selectedDatabase, String tableName) throws IOException {
        Map<Integer, String> tableMap = new HashMap<>();
        String tableFileLocation = "src/main/resources/schema/" + selectedDatabase + "/" + tableName + ".txt";
        BufferedReader reader = new BufferedReader(new FileReader(tableFileLocation));
        String line;
        int lineCount = 1;
        while ((line = reader.readLine()) != null) {
            tableMap.put(lineCount, line);
            lineCount++;
        }
        return tableMap;
    }
}
