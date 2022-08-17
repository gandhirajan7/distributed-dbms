package com.dpgten.distributeddb.erd;

import ch.qos.logback.classic.db.names.ColumnName;
import com.dpgten.GCPVmConnection;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import io.swagger.v3.oas.models.links.Link;
import io.swagger.v3.oas.models.security.SecurityScheme;

import java.io.*;
import java.lang.reflect.Array;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

public class ErdGenerator {
    private final String schema = "src\\main\\resources\\schema\\";
    private final String primarykey = "Sno";
    private final String delimeter1 = "\\|";
    private final String DELIMETER_COMMA = ",";
    private final String delimeter2 = "~";
    private final String NEW_LINE = "\n";
    private final String SEMI_COLON = ";";
    public Scanner scanner;


    public void generateRequiredERD() throws IOException, JSchException, SftpException {
        Scanner sc = new Scanner(System.in);
        System.out.println("Please enter a existing database name : ");
        String selectedDatabase = sc.nextLine();
        File file = new File(schema + "\\" + selectedDatabase);
        StringBuffer finalStringBuffer = new StringBuffer();
        if (file.exists()) {
            File[] tableList = file.listFiles();
            if (tableList.length != 0) {
                for (File table: tableList) {
                    StringBuffer stringBuffer = new StringBuffer();
                    BufferedReader reader = new BufferedReader(new FileReader(table));
                    String tableName = table.getName().replace(".txt", "");
                    stringBuffer.append("TABLE NAME : ").append(tableName).append(NEW_LINE);
                    String line;
                    List<String> columnList = null;
                    String[] columnArray = null;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("Column")) {
                            columnArray = line.split(Pattern.quote("|"));
                        }
                    }
                    String[] dataType = new String[columnArray.length];
                    for (int i=0; i < dataType.length; i++) {
                        dataType[i] = (String.format("%s", columnArray[i]));
                    }
                    String columns = String.join(", ", Arrays.copyOfRange(dataType, 1, dataType.length));
                    stringBuffer.append("COLUMNS in table : ").append(columns).append(NEW_LINE);
                    finalStringBuffer.append(stringBuffer.toString()).append(NEW_LINE);
                }
                finalStringBuffer.append("RelationShip between tables : ").append(NEW_LINE);
                for (File table: tableList) {
                    StringBuffer realtionBuffer = new StringBuffer();
                    BufferedReader reader = new BufferedReader(new FileReader(table));
                    String line;
                    String[] foreignKey = null;
                    List<String> foreignKeyList = new ArrayList<>();
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("foreign_key")) {
                            foreignKey = line.split(Pattern.quote("|"));
                            List<String> relation = new LinkedList<>(Arrays.asList(foreignKey));
                            relation.remove(0);
                            List<String> finalRealtion = null;
                            for (String s: relation) {
                                String[] arr = s.split(",");
                                finalRealtion = new LinkedList<>(Arrays.asList(arr));
                            }
                            for (String relTale: finalRealtion) {
                                String fkTable = relTale.split(Pattern.quote("->"))[0].split(Pattern.quote("$$"))[1];
                                foreignKeyList.add(fkTable);
                            }
                        }
                    }

                    if (!foreignKeyList.isEmpty()) {
                        realtionBuffer.append(table.getName().replace(".txt", ""));
                        realtionBuffer.append(foreignKeyList.get(0));
                        realtionBuffer.append(" one to many relationship -  ").append(",");
                        for (int i=0; i<foreignKeyList.size(); i++) {
                            realtionBuffer.append(foreignKeyList.get(i)).append(",");

                        }
                        finalStringBuffer.append(realtionBuffer).append(NEW_LINE);
                    }
                }
                writeToFile(finalStringBuffer, selectedDatabase);
            }
        } else {
            GCPVmConnection gcpVmConnection = new GCPVmConnection();
            ChannelSftp sftpChannel = gcpVmConnection.connectVM();
            if (gcpVmConnection.checkIfDBExistsInRemoteVM(sftpChannel, selectedDatabase)) {
                Map<String, InputStream> dataMap = new HashMap<>();
                dataMap = gcpVmConnection.getAllFilesFromDataBase(selectedDatabase, sftpChannel);
                generateERDFromVM(sftpChannel, selectedDatabase, dataMap);
            } else {
                System.out.println("Entered Database Does Not Exists !!!!");
            }
        }
    }

    private void generateERDFromVM(ChannelSftp sftpChannel, String selectedDatabase, Map<String, InputStream> dataMap) throws IOException {
        StringBuffer finalStringBuffer = new StringBuffer();
        for (Map.Entry<String, InputStream> entry: dataMap.entrySet()) {
            StringBuffer stringBuffer = new StringBuffer();
            BufferedReader reader = new BufferedReader(new InputStreamReader(entry.getValue()));
            String tableName = entry.getKey().replace(".txt", "");
            stringBuffer.append("TABLE NAME : ").append(tableName).append(NEW_LINE);
            String line;
            List<String> columnList = null;
            String[] columnArray = null;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("Column")) {
                    columnArray = line.split(Pattern.quote("|"));
                }
            }
            String[] dataType = new String[columnArray.length];
            for (int i=0; i < dataType.length; i++) {
                dataType[i] = (String.format("%s", columnArray[i]));
            }
            String columns = String.join(", ", Arrays.copyOfRange(dataType, 1, dataType.length));
            stringBuffer.append("COLUMNS in table : ").append(columns).append(NEW_LINE);
            finalStringBuffer.append(stringBuffer.toString()).append(NEW_LINE);
        }
        finalStringBuffer.append("RelationShip between tables : ").append(NEW_LINE);
        for (Map.Entry<String, InputStream> entry: dataMap.entrySet()) {
            StringBuffer realtionBuffer = new StringBuffer();
            BufferedReader reader = new BufferedReader(new InputStreamReader(entry.getValue()));
            String line;
            String[] foreignKey = null;
            List<String> foreignKeyList = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("foreign_key")) {
                    foreignKey = line.split(Pattern.quote("|"));
                    List<String> relation = new LinkedList<>(Arrays.asList(foreignKey));
                    relation.remove(0);
                    List<String> finalRealtion = null;
                    for (String s: relation) {
                        String[] arr = s.split(",");
                        finalRealtion = new LinkedList<>(Arrays.asList(arr));
                    }
                    for (String relTale: finalRealtion) {
                        String fkTable = relTale.split(Pattern.quote("->"))[0].split(Pattern.quote("$$"))[1];
                        foreignKeyList.add(fkTable);
                    }
                }
            }

            if (!foreignKeyList.isEmpty()) {
                realtionBuffer.append(entry.getKey().replace(".txt", ""));
                realtionBuffer.append(foreignKeyList.get(0));
                realtionBuffer.append(" one to many relationship -  ").append(",");
                for (int i=0; i<foreignKeyList.size(); i++) {
                    realtionBuffer.append(foreignKeyList.get(i)).append(",");

                }
                finalStringBuffer.append(realtionBuffer).append(NEW_LINE);
            }
        }
        writeToFileInVM(finalStringBuffer, selectedDatabase);

    }

    private void writeToFile(StringBuffer finalStringBuffer, String selectedDatabase) throws IOException {
        String generatedErdLocation = "src\\main\\resources\\ERD";
        File file = new File(generatedErdLocation);
        if (!file.exists()) {
            file.mkdir();
        }
        File dumpPath = new File(generatedErdLocation + "\\" + selectedDatabase + ".txt");
        dumpPath.createNewFile();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dumpPath, true));
        bufferedWriter.append(finalStringBuffer.toString());
        bufferedWriter.flush();

    }

    private void writeToFileInVM(StringBuffer finalStringBuffer, String selectedDatabase) throws IOException {
        String generatedErdLocation = "/home/avuser/erd";
        File file = new File(generatedErdLocation);
        if (!file.exists()) {
            file.mkdir();
        }
        File dumpPath = new File(generatedErdLocation + "/" + selectedDatabase + ".txt");
        dumpPath.createNewFile();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dumpPath, true));
        bufferedWriter.append(finalStringBuffer.toString());
        bufferedWriter.flush();

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
