package com.dpgten.distributeddb.query;

import com.dpgten.distributeddb.analytics.DatabaseAnalytics;
import com.dpgten.distributeddb.utils.FileResourceUtils;
import com.dpgten.distributeddb.utils.MetadataUtils;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.dpgten.distributeddb.query.QueryParser.*;
import static com.dpgten.distributeddb.utils.Utils.*;

public class TableQuery {


    public List<String> selectRows(String inputQuery) {
        Matcher selectRowsMatcher = SELECT_TABLE_WHERE_PATTERN.matcher(inputQuery);
        String tablePath = "";
        List<String> selectRows = new ArrayList<>();
        if (selectRowsMatcher.find()) {
            MetadataUtils mdUtils = new MetadataUtils();
            tablePath = mdUtils.getTablePath(selectRowsMatcher.group(8));
//            String instance = mdUtils.getVMInstance(selectRowsMatcher.group(8));
//            String [] result= restCallController.selectRestCall(inputQuery, instance);
        }

        File tableFile = new File(tablePath);

        if (selectRowsMatcher.group(9) != null) {
            String columnName = selectRowsMatcher.group(10);
            String columnValue = selectRowsMatcher.group(11);
            selectRows = executeWhere(tableFile, columnName, columnValue, inputQuery);
//            selectRows.forEach(System.out::println);
            DatabaseAnalytics.SELECT_QUERY_COUNT++;
        }else{
            FileResourceUtils fileUtils = new FileResourceUtils();
            fileUtils.printFile(tableFile);
        }
        return selectRows;
    }

    public void updateRow(String inputQuery) {
        Matcher updateQueryMatcher = UPDATE_TABLE_PATTERN.matcher(inputQuery);
        String tablePath = "";

        if (updateQueryMatcher.find()) {
            MetadataUtils mdUtils = new MetadataUtils();
            tablePath = mdUtils.getTablePath(updateQueryMatcher.group(1));
        }
        File tableFile = new File(tablePath);
        if (updateQueryMatcher.group(4) != null) {
            String columnName = updateQueryMatcher.group(5);
            String columnValue = updateQueryMatcher.group(6);
            List<String> selectRows = executeWhere(tableFile, columnName, columnValue, inputQuery);
            StringBuilder updatedFile = new StringBuilder();
            try {
                Scanner tableScanner = new Scanner(tableFile);
                updatedFile.append(tableScanner.nextLine()).append("\n")
                        .append(tableScanner.nextLine()).append("\n").append(tableScanner.nextLine());
                tableScanner.close();
                updatedFile.append("\n").append(selectRows.stream().map(Object::toString)
                        .collect(Collectors.joining("\n")));
                try {
                    FileWriter tableWriter = new FileWriter(tableFile);
                    tableWriter.write(updatedFile.toString());
                    tableWriter.close();
                    DatabaseAnalytics.UPDATE_QUERY_COUNT++;
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            } catch (FileNotFoundException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public List<String> executeWhere(File tableFile, String columnName
            , String columnValue, String inputQuery) {
        List<String> selectRows = new ArrayList<>();
        String operation;
        QueryValidator validator = new QueryValidator();
        if (validator.isUpdateQuery(inputQuery)) {
            operation = "update";
        } else {
            operation = "select";
        }
        try {
            Scanner tableScanner = new Scanner(tableFile);
            tableScanner.nextLine();
            tableScanner.nextLine();
            String columnHeader = tableScanner.nextLine();
            String[] headers = columnHeader.split(PRIMARY_DELIMITER_REGEX);
            List<String> headerArray = new ArrayList<>();
            for (String header : headers) {
                headerArray.add(header.split(",")[0]);
            }

            //todo fix ArrayIndexOutOFBound Error = -1
            int columnIndex = headerArray.indexOf(columnName);

            while (tableScanner.hasNext()) {
                String row = tableScanner.nextLine();
                if (row.split(PRIMARY_DELIMITER_REGEX)[columnIndex].equals(columnValue)) {
                    if (operation.equals("select")) {
                        selectRows.add(row);
                    } else if (operation.equals("update")) {
                        Matcher updateQueryMatcher = UPDATE_TABLE_PATTERN.matcher(inputQuery);
                        if (updateQueryMatcher.find()) {
                            String matchColumnName = updateQueryMatcher.group(2);
                            int matchColumnIndex = headerArray.indexOf(matchColumnName);
                            String matchColumnValue = updateQueryMatcher.group(3);
                            String[] rowValues = row.split(PRIMARY_DELIMITER_REGEX);
                            rowValues[matchColumnIndex] = matchColumnValue;
                            selectRows.add(String.join(PRIMARY_DELIMITER, rowValues));
                        }
                    }
                } else {
                    if (operation.equals("update") || operation.equals("delete")) {
                        selectRows.add(row);
                    }
                }
            }
            tableScanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return selectRows;
    }

    public boolean searchTable(String tableName, String database) {
        String[] tables = new File(database).list();

        if (tables != null) {
            for (String table : tables) {
                if (table.equals(tableName)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void createTable(String inputQuery, String databaseName) {
        Matcher createTableMatcher = QueryParser.CREATE_TABLE_PATTERN.matcher(inputQuery);
        String tableName = null;
        String tableColumn = null;
        HashMap<String, String> tableColumnList = new HashMap<>();
        if (createTableMatcher.find()) {
            tableName = createTableMatcher.group(1);
            tableColumn = createTableMatcher.group(2);
            if (Arrays.asList(KEYWORDS).contains(tableName.toLowerCase(Locale.ROOT))
                    || Arrays.asList(KEYWORDS).contains(tableColumn.toLowerCase(Locale.ROOT))) {
                System.out.println(RED + "CANNOT USE PREDEFINED KEYWORDS" + RESET);
            }
        }

        boolean isPrimaryKey = false;
        String primaryKeyTable = "NULL";
        if (tableColumn != null) {
            for (String column : tableColumn.split(",")) {
                String columnName = column.split("\\s+")[0];
                String columnType = column.split("\\s+")[1];

                if (!isPrimaryKey && column.contains("primary_key")) {
                    isPrimaryKey = true;
                    primaryKeyTable = columnName;
                } else if (isPrimaryKey && column.contains("primary_key")) {
                    System.out.println(RED + "MULTIPLE PRIMARY KEY");
                }

                if (!tableColumnList.containsKey(columnName)) {
                    tableColumnList.put(columnName, columnType);
                } else {
                    System.out.println(RED + "DUPLICATE COLUMN NAME" + RESET);
                }
            }
        }

        File table = new File(SCHEMA + "/" + databaseName + "/" + tableName + ".txt");

        if (!table.exists()) {
            try {
                table.createNewFile();
            } catch (IOException e) {
                System.out.println("ERROR IN CREATION OF THE TABLE");
                System.out.println(e.getMessage());
            }
        } else {
            System.out.println("Table " + tableName + " already exists");
        }

        try {
            FileWriter writeHeader = new FileWriter(table);
            StringBuilder columnWriterString = new StringBuilder("");

            columnWriterString.append("primary_key").append(PRIMARY_DELIMITER).append(primaryKeyTable).append("\n")
                    .append("foreign_key|name").append("\n");

            for (String columnName : tableColumnList.keySet()) {
                columnWriterString.append("Column|").append(columnName).append(SECONDARY_DELIMITER)
                        .append(tableColumnList.get(columnName)).append(PRIMARY_DELIMITER);
            }

            writeHeader.write(String.valueOf(columnWriterString.deleteCharAt(columnWriterString.length() - 1)));
            writeHeader.close();

            MetadataUtils mdUtils = new MetadataUtils();
            mdUtils.createTableEntry(tableName,databaseName);
            DatabaseAnalytics.CREATE_QUERY_COUNT++;
        } catch (IOException e) {
            System.out.println("ERROR IN INSERTING THE COLUMNS");
            System.out.println(e.getMessage());
        }
    }

    public boolean insertRow(String inputQuery) {
        List<String[]> line = new ArrayList<>();
        Matcher queryMatcher = INSERT_TABLE_PATTERN.matcher(inputQuery);
        MetadataUtils mdUtils = new MetadataUtils();
        String tablePath = "";

        if (queryMatcher.find()) {
            String tableName = queryMatcher.group(1);
            tablePath = mdUtils.getTablePath(tableName);
        }
        try (BufferedReader br = new BufferedReader(new FileReader(tablePath))) {
            br.readLine();
            br.readLine();
            String columnNamesLine = br.readLine();
            List<String> columns = Arrays.asList(columnNamesLine.split(PRIMARY_DELIMITER_REGEX));
            List<String> columnNames = new ArrayList<>();

            for (String column : columns) {
                columnNames.add(column.split(",")[0]);
            }

            int i = 0;
            boolean increase = false;
            increase = true;
            List<String> currentCol = Arrays.asList(queryMatcher.group(2).split(","));
            String[] currentVal = queryMatcher.group(3).split(",");
            String[] values = new String[currentVal.length + 1];
            values[0] = "Row";
            line.add(values);
            int vIndex = 0;
            for (String col : currentCol) {
                int index = columnNames.indexOf(col.trim());
                if (index != -1) {
                    line.get(i)[index] = currentVal[vIndex++];
                } else {
                    System.out.println("Column Name Mismatch.");
                    line.remove(line.size() - 1);
                    increase = false;
                    break;
                }
            }
            if (increase) {
                i++;
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try(BufferedWriter bw=new BufferedWriter(new FileWriter(tablePath,true))){
            for(String[] list:line) {
                List<String> alist=Arrays.asList(list);
               bw.append("\n");
               bw.append(alist.stream().map(Object::toString).collect(Collectors.joining(PRIMARY_DELIMITER)));
               DatabaseAnalytics.INSERT_QUERY_COUNT++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}
