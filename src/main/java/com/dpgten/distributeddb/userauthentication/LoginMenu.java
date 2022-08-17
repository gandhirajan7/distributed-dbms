package com.dpgten.distributeddb.userauthentication;

import com.dpgten.distributeddb.query.QueryImpl;
import com.dpgten.distributeddb.utils.FileResourceUtils;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;

import static com.dpgten.distributeddb.userauthentication.AlgorithmHashSHA.compareSHA256Hash;
import static com.dpgten.distributeddb.utils.Utils.PRIMARY_DELIMITER;

/**
 * Authentication Login Menus
 *
 * @author DMWA Group 10
 */
@Data
@Component
public class LoginMenu {

    public static String USER_NAME;

    FileResourceUtils fileResourceUtils;
    private Scanner scanner;
    private String delimeter = "$\\|$";
    @Autowired
    private UserSession userSession;
    private User user;

    private File file;

    private String userProfile = "src//main//resources//UserProfiles.txt";

    /**
     * Default Constructor
     */
    public LoginMenu() {
        scanner = new Scanner(System.in);
        fileResourceUtils = new FileResourceUtils();
        System.out.println("-------------------------------------------------------");
        System.out.println("------Welcome to dpg-10 Distributed Database-----------");
        System.out.println("-------------------------------------------------------");
        file = new File(userProfile);
        user = new User();
    }

    /**
     * Displays first selection menu.
     *
     * @throws IOException          handles the file not found etc.
     * @throws InterruptedException handles the interruption errors.
     */
    public void userFirstMenu() {
        boolean loopCheck = true;
        while (loopCheck) {
            System.out.println("\nLogin Menu");
            System.out.println("Press 1 ==> User Login\nPress 2 ==> Register new user\nPress 3 ==> Exit");
            System.out.print("Enter your Choice::: ");
            String selectedOption = scanner.nextLine();
            switch (selectedOption) {
                case "1":
                    boolean result = attemptLogin();
                    if (!result) {
                        System.out.println("Invalid credentials/User does not exist");
                    } else {
                        System.out.println("Calling query executor");
                        QueryImpl queryImpl = new QueryImpl();
                        MainMenu menu = new MainMenu();
                        loopCheck = menu.drive(user);
//                        loopCheck = queryImpl.executeQuery(user);
//                        loopCheck = false;
                    }
                    break;
                case "2":
                    registerNewUser();
                    break;
                case "3":
                    loopCheck = logout();
                    break;
                default:
                    System.out.println("Invalid input please try again!!");
            }
        }
        //below line needs to be removed.
        System.exit(1);
    }


    /**
     * Validates user login page
     *
     * @throws IOException          handles the file not found etc.
     * @throws InterruptedException handles the interruption errors.
     */
    private boolean attemptLogin() {
        System.out.print("Enter username::");
        user.setUsername(scanner.nextLine());
        System.out.print("Enter password::");
        String enteredPassword = scanner.nextLine();
        BufferedReader bufferedReader = null;
        boolean loginChecker = false;
        InputStream inputStream = fileResourceUtils.getFileFromResourceAsStream("UserProfiles.txt");

        try (InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] lineSingleUserArray = line.split("\\|");
                if (lineSingleUserArray.length > 4) {
                    if (compareSHA256Hash(user.getUsername(), lineSingleUserArray[0].trim())) {
                        if (compareSHA256Hash(enteredPassword, lineSingleUserArray[1].trim())) {
                            loginChecker = true;
                            break;
                        }
                    }
                }
            }

        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return loginChecker;
    }


    private void registerNewUser() {
        System.out.print("\nPreparing to register a new user\n");
        try {
            System.out.print("Enter Username: ");
            String userName = scanner.nextLine();
            user.setUsername(userName);
            if (userExists()) {
                System.out.println("User already exists.. Try registering again!!\n");
                return;
            }
            user.setHashedUsername(AlgorithmHashSHA.convertSHA256Hash(userName));
            System.out.print("Enter Password: ");
            user.setHashedPassword(AlgorithmHashSHA.convertSHA256Hash(scanner.nextLine()));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        System.out.print("Enter Security question: ");
        user.setSecurityQuestion(scanner.nextLine());
        System.out.print("Enter Answer: ");
        user.setSecurityAnswer(scanner.nextLine());
        System.out.println("User " + user.getUsername() + " was registered Successfully.");
        userRegistrationAddFile();
        scanner.close();
    }

    /**
     * Adds the user info from registration input method
     *
     * @throws IOException handles the file not found etc.
     */
    public boolean userRegistrationAddFile() {
        String line = user.getHashedUsername() + PRIMARY_DELIMITER + user.getHashedPassword() + PRIMARY_DELIMITER + user.getSecurityQuestion() + PRIMARY_DELIMITER + user.getSecurityAnswer() + PRIMARY_DELIMITER + "N";
        return FileResourceUtils.appendToFile(userProfile, line);
    }

    public Boolean userExists() {
        boolean result = false;
        BufferedReader bufferedReader = null;
        String line = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while (!result) {
            try {
                if ((line = bufferedReader.readLine()) == null) break;
                String[] lineSingleUser = line.split(PRIMARY_DELIMITER);
                if ((compareSHA256Hash(user.getUsername(), lineSingleUser[0]))) {
                    result = true;
                }
            } catch (NoSuchAlgorithmException | IOException e) {
                e.printStackTrace();
            }

        }
        return result;
    }

    public boolean logout() {
        user.setLoggedIn("N");
        // write code to update file.
        System.out.println("\nLogging out, Have a great day.");
        return false;
    }

    void afterLoginMenu() {
        AccessMenu.openAccessMenu();
    }
}

