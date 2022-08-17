package com.dpgten.distributeddb;

import com.dpgten.distributeddb.query.QueryImpl;
import com.dpgten.distributeddb.userauthentication.LoginMenu;
import com.dpgten.distributeddb.userauthentication.User;
import com.dpgten.distributeddb.sqldump.SqlDump;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class DistributedDbApplication {

	public static void main(String[] args) throws IOException {


		SpringApplication.run(DistributedDbApplication.class, args);

		// Entry point to application
		LoginMenu lm = new LoginMenu();
		lm.userFirstMenu();

	}
}
