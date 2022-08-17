package com.dpgten.distributeddb.userauthentication;

import lombok.Data;
import org.springframework.stereotype.Component;

@Component
@Data
public class UserSession {

    private String userId;

    private String selectedDb;

}
