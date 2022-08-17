package com.dpgten.logmanagement;

import com.dpgten.constants.TransactionProcessingConstants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

public class EventLogs {
private final String EVENT_LOG_FILE_LOCATION = "src/main/resources/logs/EventLogs.txt";

    private static EventLogs eventLogsInstance = null;

    EventLogs() {
        try {
            startLogWriter();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static EventLogs getEventLogsInstance() {
        if (eventLogsInstance == null) {
            eventLogsInstance = new EventLogs();
        }
        return eventLogsInstance;
    }

    private File file;
    private BufferedWriter bufferedWriter;

    public void startLogWriter() throws IOException {
        file = new File(EVENT_LOG_FILE_LOCATION);
        file.createNewFile();
        this.bufferedWriter = new BufferedWriter(new FileWriter(file, true));
    }

    public void stopLogWriter() throws IOException {
        this.bufferedWriter.close();
    }

    public void writer(Map<String, String> eventLogMap) throws IOException {
        StringBuffer buffer = new StringBuffer();
        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date(currentTimeMillis);
        buffer.append("[").append(date).append("]").append(" ");
//        buffer.append("[");
        if (eventLogMap.containsKey(TransactionProcessingConstants.DATABASE_CHANGE_LOG)) {
            buffer.append("[").append("The New Changes in Database are : ").append(eventLogMap.get(TransactionProcessingConstants.DATABASE_CHANGE_LOG)).append("]").append(" ");
        }
        buffer.append("[");
        if (eventLogMap.containsKey(TransactionProcessingConstants.CONCURRENT_TRANSACTION_LOG)) {
            buffer.append("Current Concurrent Transaction : ").append(eventLogMap.get(TransactionProcessingConstants.CONCURRENT_TRANSACTION_LOG)).append("]").append(" ");
        }
        buffer.append("[");
        if (eventLogMap.containsKey(TransactionProcessingConstants.DATABASE_CRASH_REPORTS_LOG)) {
            buffer.append("DATABASE CRASHED : ").append(eventLogMap.get(TransactionProcessingConstants.DATABASE_CRASH_REPORTS_LOG)).append("]").append(" ");
        }
        if (eventLogMap.containsKey(TransactionProcessingConstants.OLD_VALUE_BEFORE_TRANSACTION)) {
            buffer.append("[").append("Previous Value before Updating : ").append(eventLogMap.get(TransactionProcessingConstants.OLD_VALUE_BEFORE_TRANSACTION)).append("]").append(" ");
        }
        if (eventLogMap.containsKey(TransactionProcessingConstants.NEW_VALUE_AFTER_TRANSACTION)) {
            buffer.append("[").append("New Value after transaction : ").append(eventLogMap.get(TransactionProcessingConstants.NEW_VALUE_AFTER_TRANSACTION)).append("]").append(" ");
        }
        this.bufferedWriter.newLine();
        this.bufferedWriter.append(buffer.toString());
        this.bufferedWriter.flush();
    }
}
