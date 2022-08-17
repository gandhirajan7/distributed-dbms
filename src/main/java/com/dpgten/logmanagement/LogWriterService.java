package com.dpgten.logmanagement;

import com.dpgten.constants.TransactionProcessingConstants;

import java.io.IOException;
import java.util.Map;

public class LogWriterService {

    private static LogWriterService logWriterServiceInstance = null;

    public static LogWriterService getLogWriterServiceInstance() {
        if (logWriterServiceInstance == null) {
            logWriterServiceInstance = new LogWriterService();
        }
        return logWriterServiceInstance;
    }

    public void write(Map<String, String> logMap) throws IOException {
        if (logMap.containsKey(TransactionProcessingConstants.QUERY_EXECUTION_TIME) && logMap.containsKey(TransactionProcessingConstants.CURRENT_DATABASE_STATE)) {
            GeneralLogs.getGeneralLogsInstance().writeToGeneralLogFile(logMap);
        }
        if (logMap.containsKey(TransactionProcessingConstants.EXECUTED_QUERY_BY_USER)) {
            QueryLogs.getQueryLogsInstance().writer(logMap);
        }
        if (logMap.containsKey(TransactionProcessingConstants.DATABASE_CHANGE_LOG) || logMap.containsKey(TransactionProcessingConstants.CONCURRENT_TRANSACTION_LOG)
                || logMap.containsKey(TransactionProcessingConstants.DATABASE_CRASH_REPORTS_LOG)) {
            EventLogs.getEventLogsInstance().writer(logMap);
        }
    }
}
