package com.dpgten.distributeddb.userauthentication;

import com.dpgten.distributeddb.analytics.AnalyticsOutput;
import com.dpgten.distributeddb.analytics.DatabaseAnalytics;
import com.dpgten.distributeddb.erd.ErdGenerator;
import com.dpgten.distributeddb.query.QueryImpl;
import com.dpgten.distributeddb.sqldump.SqlDump;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Scanner;

import static com.dpgten.distributeddb.utils.Utils.*;
import static com.dpgten.distributeddb.utils.Utils.RESET;

@Data
@Component
public class MainMenu {

    public boolean drive(User user) {
        System.out.println("\n" + YELLOW + "-----------------------Welcome " + user.getUsername() + "---------------------" + RESET);
        System.out.println(BLUE + "AUTHENTICATION SUCCESS" + RESET);
        boolean loopCheck = true;
        Scanner scanner = new Scanner(System.in);
        while (loopCheck) {
            System.out.println("\nMain Menu");
            System.out.println("Press 1 ==> Process Query\nPress 2 ==> Generate SQL Dump\nPress 3 ==> Generate ERD \nPress 4 ==> View Analytics\nPress 5 ==> Logout");
            System.out.print("Enter your Choice::: ");
            String selectedOption = scanner.nextLine();
            switch (selectedOption) {
                case "1":
                    QueryImpl impl = new QueryImpl();
                    boolean result = impl.executeQuery(user);
                    break;
                case "2":
                    SqlDump dump = new SqlDump();
                    System.out.print(YELLOW+"Enter database Name:"+RESET);
                    String databaseName = scanner.nextLine();
                    try {
                        dump.generateDump(databaseName);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (JSchException e) {
                        e.printStackTrace();
                    } catch (SftpException e) {
                        e.printStackTrace();
                    }
                    break;
                case "3":
                    //ERD generator
                    ErdGenerator erdGenerator = new ErdGenerator();
                    try {
                        erdGenerator.generateRequiredERD();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (JSchException e) {
                        e.printStackTrace();
                    } catch (SftpException e) {
                        e.printStackTrace();
                    }
                    System.out.println("generating ERD");
                    break;
                case "4":
                    AnalyticsOutput da = new AnalyticsOutput();
                    //View Analytics
//                    System.out.println("Fetch data from file");
                    break;
                case "5":
                    loopCheck = false;
                    break;
                default:
                    System.out.println("Invalid input try again");
            }
        }
        return true;
    }
}
