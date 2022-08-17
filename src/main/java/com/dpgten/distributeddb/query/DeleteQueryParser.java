package com.dpgten.distributeddb.query;

import com.dpgten.distributeddb.userauthentication.User;
import com.dpgten.distributeddb.utils.FileResourceUtils;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static com.dpgten.distributeddb.utils.Utils.LOCAL_STORAGE_PATH;
import static com.dpgten.distributeddb.utils.Utils.PRIMARY_DELIMITER_REGEX;

@Component
public class DeleteQueryParser {

    private FileResourceUtils fileResourceUtils;

    private String currentDatabase;

    // result 0 is error.
    // result 1 is success.
    // result 2 is database does not exist.
    // result 3 is table does not exist.

    DeleteQueryParser() {
        fileResourceUtils = new FileResourceUtils();
    }


    public int executeDeleteQueryWithConditionQuery(String query, User user) {
        Integer result = 1;
        query = "Delete FROM tb1 where sno = 1;";
        System.out.println("\n\nDelete Query started" + query);

        String[] queryArray = query.substring(0, query.length() - 1).split(" ");
        String tableName = queryArray[2].trim();
        String whereColumn = "";
        String whereConditionValue = "";
        if (queryArray.length > 3) {
            whereColumn = queryArray[4].trim();
            whereConditionValue = queryArray[6].trim();
        } else {
            return 1;
        }

        currentDatabase = user.getCurrentDatabase();
        if (fileResourceUtils.checkIfDatabaseExists(currentDatabase)) {
            if (fileResourceUtils.checkIfTableExists(currentDatabase, tableName)) {
                    String filePath = LOCAL_STORAGE_PATH + "//" + currentDatabase + "//" + tableName +".txt";
//                String filePath = "schema//" + currentDatabase + "//" + tableName + ".txt";
//                File table = fileResourceUtils.getFileFromResource(filePath);
                File table = new File(filePath);
                List<String> deletedRows = executeDeleteWhere(table, whereColumn, whereConditionValue);
                return 1;
            } else {
                return 3;
            }
        } else {
            return 2;
        }
    }

    public List<String> executeDeleteWhere(File tableFile, String columnName, String columnValue) {
        List<String> deletedRows = new ArrayList<>();
        List<String> replacementTableData = new ArrayList<>();
        final StringBuilder stringBuilder = new StringBuilder();
        try (final FileReader fileReader = new FileReader(tableFile.getPath());
             final BufferedReader bufferedReader = new BufferedReader(fileReader)) {
//            Scanner tableScanner = new Scanner(tableFile);
            String primaryKeyDetails = bufferedReader.readLine();
//            String primaryKeyDetails = tableScanner.nextLine();
            stringBuilder.append(primaryKeyDetails + "\n");
            int columnIndex = -1;
            String columnHeader = bufferedReader.readLine();
            stringBuilder.append(columnHeader + "\n");
            String[] headers = columnHeader.split("\\|");
            List<String> headerArray = new ArrayList<>();
            List<String> headerArrayLowerCase = new ArrayList<>();
            for (String header : headers) {
                headerArray.add(header.split(",")[0].trim());
                headerArrayLowerCase.add(header.split(",")[0].toLowerCase().trim());
            }
            columnIndex = headerArrayLowerCase.indexOf(columnName.toLowerCase());
            String row;
            while ((row = bufferedReader.readLine()) != null) {
                String tempArr = row.split(PRIMARY_DELIMITER_REGEX)[columnIndex];
                if (row.split(PRIMARY_DELIMITER_REGEX)[columnIndex].equals(columnValue)) {
                    deletedRows.add(row);
                } else {
                    stringBuilder.append(row + "\n");
                    replacementTableData.add(row);
                }
            }
//            File file = new File(tableFile);
//            FileWriter fw = null;
            File file = new File(tableFile.getPath());
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(String.valueOf(stringBuilder));
            bw.flush();
            fw.close();
            bw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return deletedRows;

    }
}

