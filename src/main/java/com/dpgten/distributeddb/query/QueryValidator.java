package com.dpgten.distributeddb.query;

import static com.dpgten.distributeddb.query.QueryParser.*;
import static com.dpgten.distributeddb.query.QueryParser.WHERE_CONDITION_PATTERN;

public class QueryValidator {
    public boolean isUseQuery(String inputQuery){
        return USE_DATABASE_PATTERN.matcher(inputQuery).find();
    }

    public boolean isCreateTableQuery(String inputQuery){
        return CREATE_TABLE_PATTERN.matcher(inputQuery).find();
    }

    public boolean isCreateQuery(String inputQuery){
        return CREATE_DATABASE_PATTERN.matcher(inputQuery).find();
    }

    public boolean isSelectQuery(String inputQuery){
        return SELECT_TABLE_WHERE_PATTERN.matcher(inputQuery).find();
    }

    public boolean isInsertQuery(String inputQuery){
        return INSERT_TABLE_PATTERN.matcher(inputQuery).find();
    }

    public boolean isUpdateQuery(String inputQuery){
        return UPDATE_TABLE_PATTERN.matcher(inputQuery).find();
    }

    public boolean isWhereCondition(String inputQuery){
        return WHERE_CONDITION_PATTERN.matcher(inputQuery).find();
    }
}
