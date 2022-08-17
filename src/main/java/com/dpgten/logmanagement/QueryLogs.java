package com.dpgten.logmanagement;

import com.dpgten.constants.TransactionProcessingConstants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;

public class QueryLogs {

    private final String QUERY_LOG_FILE_LOCATION = "src/main/resources/logs/QueryLogs.txt";

    private static QueryLogs queryLogsInstance = null;

    QueryLogs() {
        try {
            startLogWriter();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static QueryLogs getQueryLogsInstance() {
        if (queryLogsInstance == null) {
            queryLogsInstance = new QueryLogs();
        }
        return queryLogsInstance;
    }

    private File file;
    private BufferedWriter bufferedWriter;

    public void startLogWriter() throws IOException {
        file = new File(QUERY_LOG_FILE_LOCATION);
        file.createNewFile();
        this.bufferedWriter = new BufferedWriter(new FileWriter(file, true));
    }

    public void stopLogWriter() throws IOException {
        this.bufferedWriter.close();
    }

    public void writer(Map<String, String> queryLogMap) throws IOException {
        StringBuffer buffer = new StringBuffer();
        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date(currentTimeMillis);
        buffer.append("[").append(date).append("]").append(" ");
        buffer.append("[");
        if (queryLogMap.containsKey(TransactionProcessingConstants.EXECUTED_QUERY_BY_USER)) {
            buffer.append("Executed Query : ").append(queryLogMap.get(TransactionProcessingConstants.EXECUTED_QUERY_BY_USER)).append("]").append(" ");
        }
        buffer.append("[");
        if (queryLogMap.containsKey(TransactionProcessingConstants.QUERY_SUBMISSION_TIME)) {
            buffer.append("Query Submission Time : ").append(queryLogMap.get(TransactionProcessingConstants.QUERY_SUBMISSION_TIME)).append("]").append(" ");
        }
        buffer.append("[");
        if (queryLogMap.containsKey(TransactionProcessingConstants.QUERY_EXECUTION_STATUS)) {
            buffer.append("Query Execution Status : ").append(queryLogMap.get(TransactionProcessingConstants.QUERY_EXECUTION_STATUS)).append("]").append(" ");
        }
        if (queryLogMap.containsKey(TransactionProcessingConstants.QUERY_EXECUTED_ON_TABLE)) {
            buffer.append("Query on Table : ").append(queryLogMap.get(TransactionProcessingConstants.QUERY_EXECUTED_ON_TABLE)).append("]").append(" ");
        }
        if (queryLogMap.containsKey(TransactionProcessingConstants.DATA_CHANGED_BY_QUERY)) {
            buffer.append("[").append("Data Changed by query : ").append(queryLogMap.get(TransactionProcessingConstants.DATA_CHANGED_BY_QUERY)).append("]").append(" ");
        }
        if (queryLogMap.containsKey(TransactionProcessingConstants.QUERY_EXECUTION_TIME)) {
            buffer.append("[").append("Query Execution Time : ").append(queryLogMap.get(TransactionProcessingConstants.QUERY_EXECUTION_TIME)).append("]").append(" ");
        }
        this.bufferedWriter.newLine();
        this.bufferedWriter.append(buffer.toString());
        this.bufferedWriter.flush();
    }
}
