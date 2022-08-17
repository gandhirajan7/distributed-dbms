package com.dpgten.logmanagement;

import com.dpgten.constants.TransactionProcessingConstants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

public class GeneralLogs {

    private final String GENERAL_LOG_FILE_LOCATION = "src/main/resources/logs/GeneralLogs.txt";

    private static GeneralLogs generalLogsInstance = null;

    GeneralLogs() {
        try {
            startLogWriter();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static GeneralLogs getGeneralLogsInstance() {
        if (generalLogsInstance == null) {
            generalLogsInstance = new GeneralLogs();
        }
        return generalLogsInstance;
    }

    private File file;
    private BufferedWriter bufferedWriter;

    public void startLogWriter() throws IOException {
        file = new File(GENERAL_LOG_FILE_LOCATION);
        file.createNewFile();
        this.bufferedWriter = new BufferedWriter(new FileWriter(file, true));
    }

    public void stopLogWriter() throws IOException {
        this.bufferedWriter.close();
    }

    public void writeToGeneralLogFile(Map<String, String> logMap) throws IOException {
        StringBuffer buffer = new StringBuffer();
        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date(currentTimeMillis);
        buffer.append("[").append(date).append("]").append(" ");
        buffer.append("[").append("Query Execution Time : ").append(logMap.get(TransactionProcessingConstants.QUERY_EXECUTION_TIME)).append("]")
                .append(" ");
        buffer.append("[").append("Current Database State : ").append(logMap.get(TransactionProcessingConstants.CURRENT_DATABASE_STATE)).append("]");
        this.bufferedWriter.newLine();
        this.bufferedWriter.append(buffer.toString());
        this.bufferedWriter.flush();
    }
}
