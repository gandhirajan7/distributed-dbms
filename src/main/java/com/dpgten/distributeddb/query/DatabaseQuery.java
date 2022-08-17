package com.dpgten.distributeddb.query;

import com.dpgten.distributeddb.analytics.DatabaseAnalytics;
import com.dpgten.distributeddb.utils.FileResourceUtils;
import com.dpgten.distributeddb.utils.MetadataUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;

import static com.dpgten.distributeddb.query.QueryParser.*;
import static com.dpgten.distributeddb.utils.Utils.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DatabaseQuery {
    String databasePath;
    String databaseName;
    String currentUser;
    File server1;
    File server2;
    String database1Path;
    String database2Path;
    DatabaseAnalytics databaseAnalytics = new DatabaseAnalytics();


    FileResourceUtils fileResourceUtils = new FileResourceUtils();
    MetadataUtils metadataUtils = new MetadataUtils();

    public DatabaseQuery(String server1, String server2, String currentUser) {
        this.server1 = new File(server1);
        this.server2 = new File(server2);
        this.currentUser = currentUser;
    }

    public void createDatabase(String inputQuery) {
        Matcher createQueryMatcher = CREATE_DATABASE_PATTERN.matcher(inputQuery);

        if (createQueryMatcher.find()) {
            this.databaseName = createQueryMatcher.group(1);

            //todo decide in which server need to create the database
            File database = new File(SCHEMA + "/" + databaseName);

            MetadataUtils mdUtils = new MetadataUtils();
            if (!mdUtils.isDatabaseExist(databaseName)) {
                database.mkdir();
//                fileResourceUtils.appendToFile(database, "VM1|")
                System.out.println("database " + databaseName + " Created.");
                databaseAnalytics.setDATABASE_QUERY_COUNT(databaseAnalytics.DATABASE_QUERY_COUNT+1);
            } else {
                System.out.println("database " + databaseName + " already exists");
            }
        }
    }

    public boolean isDeleteQuery(String inputQuery) {
        Matcher queryMatcher = DELETE_QUERY_PATTERN.matcher(inputQuery);
        return queryMatcher.find();
    }

    public String selectDatabase(String selectDb) {
        Matcher useQueryMatcher = USE_DATABASE_PATTERN.matcher(selectDb);
        String databaseName = "";

        if (useQueryMatcher.find()) {
            databaseName = useQueryMatcher.group(1);
            if (Arrays.stream(KEYWORDS).allMatch(databaseName.toLowerCase(Locale.ROOT)::equals)) {
                System.out.println(RED + "CANNOT USE PREDEFINED KEYWORDS" + RESET);
            }
        } else {
            System.out.println(RED + "DATABASE NOT FOUND" + RESET);
        }
//
//        String currentUserPath1 = searchUser(server1,currentUser);
//        String currentUserPath2 = searchUser(server2,currentUser);

//        if(!currentUserPath1.equals("")){
        this.databasePath = searchDatabase(SCHEMA, databaseName);
//        this.databasePath = searchDatabase(SCHEMA, databaseName);
        boolean exists = metadataUtils.isDatabaseExist(databaseName);
        if (!exists) {
            System.out.println("Database does not exists");
            return "";
        }
        return databaseName;
    }

    public String searchDatabase(String currentUser, String currentDatabase) {
        String[] databases = new File(currentUser).list();
        if (databases != null) {
            for (String database : databases) {
                if (database.equals(currentDatabase)) {
                    return currentUser + "/" + currentDatabase;
                }
            }
        }
        return "";
    }

    public String searchUser(File server, String currentUser) {
        String[] users = server.list();
        if (users != null) {
            for (String user : users) {
                if (user.equals(currentUser)) {
                    return server + "/" + user;
                }
            }
        }
        return "";
    }
}
