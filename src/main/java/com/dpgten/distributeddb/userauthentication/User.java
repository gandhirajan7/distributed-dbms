package com.dpgten.distributeddb.userauthentication;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
    private String username;

    private String hashedUsername;

    private String hashedPassword;

    private String securityQuestion;

    private String securityAnswer;

    private String loggedIn;

    private String currentDatabase;
}
