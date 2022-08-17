package com.dpgten.distributeddb.utils;

public class Utils {
    public static final String SERVERS = "src/main/java/com/dpgten/distributeddb/servers";
    public static final String SERVER_1 = SERVERS + "/server1";
    public static final String SERVER_2 = SERVERS + "/server2";

    public static final String VM_1 = "35.239.173.17";
    public static final String VM_2 = "35.226.40.140";

    public static final String LOCAL_STORAGE_PATH = "src//main//resources//schema";
    public static final String SCHEMA= "src/main/resources/schema";

    public static final String SERVER1_NAME = "server1";
    public static final String SERVER2_NAME = "server2";

    public static final String GLOBAL_METADATA = "src/main/resources/GlobalMetaData.txt";

    public static final String PRIMARY_DELIMITER = "|";
    public static final String PRIMARY_DELIMITER_REGEX = "\\|";
    public static final String SECONDARY_DELIMITER = ",";

    public static final String GREEN = "\033[0;32m";
    public static final String YELLOW = "\033[0;33m";
    public static final String BLUE = "\033[0;34m";
    public static final String WHITE = "\033[0;37m";
    public static final String RED = "\033[0;31m";
    public static final String RESET = "\033[0m";


    public static final String[] TYPES = {"int","varchar","boolean"};
    public static final String[] KEYWORDS = {"database","table","create","insert","update","delete","use"
            ,"int","varchar","boolean","primary key","null"};
}
