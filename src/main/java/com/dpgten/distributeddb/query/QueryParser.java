package com.dpgten.distributeddb.query;

import java.util.regex.Pattern;

public class QueryParser {
    public static String USE_DATABASE = "USE\\s(\\w+);";
    public static final Pattern USE_DATABASE_PATTERN = Pattern.compile(USE_DATABASE);

    public static String CREATE_DATABASE = "CREATE\\s(\\w+);";
    public static final Pattern CREATE_DATABASE_PATTERN = Pattern.compile(CREATE_DATABASE);

    public static String CREATE_TABLE = "CREATE\\s+TABLE\\s+(\\w+)\\s*\\(((?:\\w+\\s\\w+\\(?[0-9]*\\)?,?)+)\\);";
    public static final Pattern CREATE_TABLE_PATTERN = Pattern.compile(CREATE_TABLE);

    public static String SELECT_TABLE_WHERE = "SELECT\\s+((\\*)?((\\w+)?((,(\\w+))*)?))\\s+FROM\\s+(\\w+)" +
            "(\\s+WHERE\\s+(\\w+)\\s+=\\s+(\\w+))*;";
//    public static String SELECT_TABLE = "^(?i)(SELECT\\s[a-zA-Z\\d]+(,\\s[a-zA-Z\\d]+)*\\sFROM\\s[a-zA-Z\\d]+;)$";
    public static final Pattern SELECT_TABLE_WHERE_PATTERN = Pattern.compile(SELECT_TABLE_WHERE);

    public static String INSERT_TABLE = "INSERT\\sINTO\\s(\\w+)\\s\\(([\\s\\S]+)\\)\\sVALUES\\s\\(([\\s\\S]+)\\);";
    public static final Pattern INSERT_TABLE_PATTERN = Pattern.compile(INSERT_TABLE);

    public static String SELECT_TABLE = "SELECT\\s+((\\*)?((\\w+)?((,(\\w+))*)?))\\s+FROM\\s+(\\w+)";
    public static final Pattern  SELECT_TABLE_PATTERN = Pattern.compile(SELECT_TABLE);

    public static String UPDATE_TABLE = "UPDATE\\s+TABLE\\s+(\\w+)\\s+SET\\s+(\\w+)\\s+=\\s+(\\w+)\\s*" +
            "(WHERE\\s+(\\w+)\\s+=\\s+(\\w+))*;";
    public static final Pattern UPDATE_TABLE_PATTERN = Pattern.compile(UPDATE_TABLE);

    public static String WHERE_CONDITION = "WHERE\\s+(\\w+)\\s+=\\s+(\\w+);";
    public static final Pattern WHERE_CONDITION_PATTERN = Pattern.compile(WHERE_CONDITION);

    public static final String DELETE_TABLE_QUERY=  "DELETE";
    public static final Pattern DELETE_TABLE_PATTERN = Pattern.compile(DELETE_TABLE_QUERY);

    public static final String DELETE_QUERY_WITH_CONDITION= "^(?i)(DELETE\\s.*FROM\\s.*WHERE\\s.*)$";
    public static final Pattern DELETE_QUERY_PATTERN = Pattern.compile(DELETE_QUERY_WITH_CONDITION);
}

