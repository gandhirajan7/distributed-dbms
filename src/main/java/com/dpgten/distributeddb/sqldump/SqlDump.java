package com.dpgten.distributeddb.sqldump;

import com.dpgten.GCPVmConnection;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import io.swagger.v3.oas.models.security.SecurityScheme;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

public class SqlDump {

    //    private final String schema = "schema";
    private final String schema = "src\\main\\resources\\schema\\";

    private final String primarykey = "Sno";
    private final String delimeter1 = "\\|";
    private final String DELIMETER_COMMA = ",";
    private final String delimeter2 = "~";
    private final String NEW_LINE = "\n";
    private final String SEMI_COLON = ";";

    //RENAME VARIABLES
    public void generateDump(String selectedDatabase) throws IOException, JSchException, SftpException {
        File dataBase = new File(schema + selectedDatabase);
        if (dataBase.exists()) {
            databaseExistsToCurrentVM(selectedDatabase, dataBase);
        } else {
            GCPVmConnection gcpVmConnection = new GCPVmConnection();
            ChannelSftp sftpChannel = gcpVmConnection.connectVM();
            if (gcpVmConnection.checkIfDBExistsInRemoteVM(sftpChannel, selectedDatabase)) {
                System.out.println("DB exisits in remote VM");
                Map<String, InputStream> dataMap = new HashMap<>();
                dataMap = gcpVmConnection.getAllFilesFromDataBase(selectedDatabase, sftpChannel);
                StringBuilder builder = new StringBuilder();
                List<String> dumpQueries = new ArrayList<>();
                String createDataBaseCommand = "CREATE DATABASE " + selectedDatabase +";";
                dumpQueries.add(createDataBaseCommand);
                Map<Integer, String> currentDataMap = new HashMap<>();
                List<String> columnList = null;
                String tableName = null;
                System.out.println(dataMap.values());
                for (Map.Entry<String, InputStream> entry: dataMap.entrySet()) {
                    currentDataMap = getDataForCurrentInputStream(entry.getValue(), currentDataMap);
                    tableName = entry.getKey().replaceAll(".txt", "");
                    System.out.println("tableName : " + tableName);
                    System.out.println(currentDataMap.values());
                    StringBuffer createTableQuery = new StringBuffer();
                    createTableQuery.append("CREATE TABLE ").append(tableName).append("(");
                    String primaryKey = "";
                    for (String line: currentDataMap.values()) {
                        if (line.startsWith("Column")) {
                            columnList = new LinkedList<>(Arrays.asList(line.split(Pattern.quote("|"))));
                            columnList.remove(0);
                        }
                        if (line.startsWith("primary_key")) {
                            primaryKey = line.split(Pattern.quote("|"))[1];
                        }
                    }

                    for (String column: columnList) {
                        createTableQuery.append(column.replace(",", " ")).append(",");
                    }
                    createTableQuery.deleteCharAt(createTableQuery.toString().length()-1);
                    if (!primaryKey.isEmpty() || !primaryKey.isBlank()) {
                        createTableQuery.append(",").append("PRIMARY KEY (").append(primaryKey).append(")");
                    }
                    createTableQuery.append(");");
                    dumpQueries.add(createTableQuery.toString());
                    String insertQuery = null;
                    for (String line: currentDataMap.values()) {
                        if (line.startsWith("Row")) {
                            StringBuffer insertQueryBuffer = new StringBuffer();
                            insertQueryBuffer.append("INSERT INTO ").append(tableName).append(" ").append("(");
                            for (String column: columnList) {
                                insertQueryBuffer.append(column.split(",")[0]).append(",");
                            }
                            insertQueryBuffer.deleteCharAt(insertQueryBuffer.toString().length()-1);
                            insertQueryBuffer.append(")").append(" VALUES (");
                            String[] rowData = line.split(Pattern.quote("|"));
                            List<String> rowDataList = new LinkedList<>(Arrays.asList(rowData));
                            rowDataList.remove(0);
                            for (String data: rowDataList) {
                                insertQueryBuffer.append(data).append(",");
                            }
                            insertQueryBuffer.deleteCharAt(insertQueryBuffer.toString().length()-1);
                            insertQueryBuffer.append(");");
                            dumpQueries.add(insertQueryBuffer.toString());
                        }
                    }
                }
                writeDumpDataToFileInVM(dumpQueries, selectedDatabase, sftpChannel);
                System.out.println(builder);
            }
        }

    }

    private void databaseExistsToCurrentVM(String selectedDatabase, File dataBase) throws IOException {
        File[] tables = dataBase.listFiles();
        StringBuilder builder = new StringBuilder();
        List<String> dumpQueries = new ArrayList<>();
        String createDataBaseCommand = "CREATE DATABASE " + selectedDatabase +";";
        dumpQueries.add(createDataBaseCommand);
        Map<Integer, String> currentDataMap = new HashMap<>();
        List<String> columnList = null;
        String tableName = null;
        for (File table: tables) {
            currentDataMap = getDataForCurrentTable(table, currentDataMap);
            tableName = table.getName().replaceAll(".txt", "");
            StringBuffer createTableQuery = new StringBuffer();
            createTableQuery.append("CREATE TABLE ").append(tableName).append("(");
            String primaryKey = "";
            for (String line: currentDataMap.values()) {
                if (line.startsWith("Column")) {
                    columnList = new LinkedList<>(Arrays.asList(line.split(Pattern.quote("|"))));
                    columnList.remove(0);
                }
                if (line.startsWith("primary_key")) {
                    primaryKey = line.split(Pattern.quote("|"))[1];
                }
            }

            for (String column: columnList) {
                createTableQuery.append(column.replace(",", " ")).append(",");
            }
            createTableQuery.deleteCharAt(createTableQuery.toString().length()-1);
            if (!primaryKey.isEmpty() || !primaryKey.isBlank()) {
                createTableQuery.append(",").append("PRIMARY KEY (").append(primaryKey).append(")");
            }
            createTableQuery.append(");");
            dumpQueries.add(createTableQuery.toString());
            String insertQuery = null;
            for (String line: currentDataMap.values()) {
                if (line.startsWith("Row")) {
                    StringBuffer insertQueryBuffer = new StringBuffer();
                    insertQueryBuffer.append("INSERT INTO ").append(tableName).append(" ").append("(");
                    for (String column: columnList) {
                        insertQueryBuffer.append(column.split(",")[0]).append(",");
                    }
                    insertQueryBuffer.deleteCharAt(insertQueryBuffer.toString().length()-1);
                    insertQueryBuffer.append(")").append(" VALUES (");
                    String[] rowData = line.split(Pattern.quote("|"));
                    List<String> rowDataList = new LinkedList<>(Arrays.asList(rowData));
                    rowDataList.remove(0);
                    for (String data: rowDataList) {
                        insertQueryBuffer.append(data).append(",");
                    }
                    insertQueryBuffer.deleteCharAt(insertQueryBuffer.toString().length()-1);
                    insertQueryBuffer.append(");");
                    dumpQueries.add(insertQueryBuffer.toString());
                }
            }
        }
        writeDumpDataToFile(dumpQueries, selectedDatabase);
        System.out.println(builder);
    }

    private void writeDumpDataToFile(List<String> dumpQueries, String selectedDatabase) throws IOException {
        String dumpFileDirectory = "src\\main\\resources\\sqlDump";
        File file = new File(dumpFileDirectory);
        if (!file.exists()) {
            file.mkdir();
        }
        File dumpPath = new File(dumpFileDirectory + "\\" + selectedDatabase + ".sql");
        dumpPath.createNewFile();
        String newline = System.getProperty("line.separator");
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dumpPath, true));
        for (String query: dumpQueries) {
            bufferedWriter.append(query + newline);
        }
        bufferedWriter.flush();
    }

    private void writeDumpDataToFileInVM(List<String> dumpQueries, String selectedDatabase, ChannelSftp sftpChannel) throws IOException, SftpException {
        String dumpFileDirectory = "/home/avuser/sqlDump";
        System.out.println("creating dump in src-main-resources-sqlDump");
        File file = new File(dumpFileDirectory);
        if (!file.exists()) {
            file.mkdir();
        }
        File dumpPath = new File(dumpFileDirectory + "/" + selectedDatabase + ".sql");
        dumpPath.createNewFile();
        String newline = System.getProperty("line.separator");
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dumpPath, true));
        for (String query: dumpQueries) {
            bufferedWriter.append(query + newline);
        }
        File file1 = new File(dumpFileDirectory);
        if (file1.exists()) {
            System.out.println("directory exists :::::");
        }
        bufferedWriter.flush();
    }

    private Map<Integer, String> getDataForCurrentTable(File table, Map<Integer, String> currentDataMap) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(table));
        String line;
        Integer lineCount = 1;
        while ((line = reader.readLine()) != null) {
            currentDataMap.put(lineCount, line);
            lineCount++;
        }
        return currentDataMap;
    }

    private Map<Integer, String> getDataForCurrentInputStream(InputStream table, Map<Integer, String> currentDataMap) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(table));
        String line;
        Integer lineCount = 1;
        try {
            while ((line = reader.readLine()) != null) {
                currentDataMap.put(lineCount, line);
                lineCount++;
            }
        } catch (IOException exception) {

        }
        return currentDataMap;
    }


    public File getFileResource(String fileName) {

        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("File not found!" + fileName);
        } else {
            try {
                return new File(resource.toURI());
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    public InputStream getFileFromResourceAsStream(String fileName) {
        // The class loader that loaded the class
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(fileName);
        // the stream holding the file content
        if (inputStream == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            return inputStream;
        }

    }

}

