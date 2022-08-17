package com.dpgten.distributeddb.userauthentication;

import java.util.Scanner;

import static com.dpgten.distributeddb.userauthentication.LoginMenu.USER_NAME;

/**
 * Access Menu for selection
 * @author DMWA group 10
 */
public class AccessMenu {
    private static final Scanner scanner = new Scanner(System.in);
    public static String DATABASE_ACCESS_NAME;

    /**
     * Provides the menu after login calling from login menu
     * Database selection is optional
     */
    public static void openAccessMenu() {
        if (USER_NAME != null) {
            System.out.println("============= SELECT ONE OPTION ============");
            System.out.println("1. Write Queries \n " +
                    "2. Export\n" +
                    "3. Data Model\n" +
                    "4. Analytics\n" +
                    "5. Exit");
            System.out.print("Enter your choice: ");
            String userSelect = scanner.nextLine();
            switch (userSelect) {
                case "1":
                    System.out.println("======== SELECTED SQL QUERY =========");
                    System.out.print("Enter a SQL query: ");
                    String userQueryInput = scanner.nextLine();
                    //TODO: SQL QUERY METHOD
                    break;
                case "2":
                    if (DATABASE_ACCESS_NAME == null) {
                        System.out.println("======== SELECTED EXPORT =========");
                        System.out.println("Select database first ");
                    } else {
                        System.out.println("======== SELECTED EXPORT =========");
                        //TODO: EXPORT METHOD
                    }
                    break;
                case "3":
                    if (DATABASE_ACCESS_NAME == null) {
                        System.out.println("======== SELECTED DATA MODEL =========");
                        System.out.println("Select database first ");
                    } else {
                        System.out.println("======== SELECTED DATA MODEL =========");
                        //TODO: DATA MODEL METHOD
                    }
                    break;
                case "4":
                    if (DATABASE_ACCESS_NAME == null) {
                        System.out.println("======== SELECTED ANALYTICS =========");
                        System.out.println("Select database first ");
                    } else {
                        System.out.println("======== SELECTED ANALYTICS =========");
                        // TODO: ANALYTICS METHOD
                    }
                    break;

                case "5":
                    System.out.println("======== BYE ==========");
                    System.exit(0);
                    break;

                default:
                    System.out.println("Invalid selection. Please try again..");
                    System.exit(0);

            }
        }
    }
}
