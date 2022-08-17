package com.dpgten.distributeddb.utils;

import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

@Component
public class FileResourceUtils {

    private String schemaFolderName = "schema";

    private String schemaFullPath = "src//main//resources//schema.txt";

    public static void printInputStream(InputStream is) {

        try (InputStreamReader streamReader =
                     new InputStreamReader(is, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static String getStringInputStream(InputStream is) {
        String result = "";
        try (InputStreamReader streamReader =
                     new InputStreamReader(is, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {
            String line;
            while ((line = reader.readLine()) != null) {
                result += line;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void printFile(File file) {
        List<String> lines;
        try {
            lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
            lines.forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean appendToFile(String path, String line) {
        if (!new File(path).exists()) {
            try {
                throw new FileNotFoundException();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(path, true);
            fileWriter.append(line);
            fileWriter.append('\n');
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public InputStream getFileFromResourceAsStream(String fileName) {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(fileName);
        if (inputStream == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            return inputStream;
        }

    }

    public File getFileFromResource(String fileName) {

        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            try {
                return new File(resource.toURI());
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public boolean checkIfDatabaseExists(String databaseName) {
        boolean result = false;
        File schemaFolder = getFileFromResource(schemaFolderName);
        String[] databasesNamesList = schemaFolder.list((d, name) -> name.equalsIgnoreCase(databaseName));
        if (databasesNamesList != null && databasesNamesList.length != 0) {
            System.out.println("Database Exists");
            result = true;
        }
        return result;
    }

    public boolean checkIfTableExists(String databaseName, String tableName) {
        String finalTableName = tableName + ".txt";
        boolean result = false;
        File schemaFolder = getFileFromResource(schemaFolderName);
        File[] databasesNamesList = schemaFolder.listFiles((d, name) -> name.equalsIgnoreCase(databaseName));
        File selectedFolder = databasesNamesList[0];
//        String  = tableName;
        String[] tableNameList = selectedFolder.list((d, name) -> {
            System.out.println(name);
            return name.equalsIgnoreCase(finalTableName);
        });
        if (tableNameList != null && tableNameList.length != 0) {
            System.out.println("Table Exists");
            result = true;
        }
        return result;
    }

}
