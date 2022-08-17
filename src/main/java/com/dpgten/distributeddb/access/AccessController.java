package com.dpgten.distributeddb.access;

import com.dpgten.distributeddb.query.TableQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static com.dpgten.distributeddb.utils.Utils.GLOBAL_METADATA;

@RestController
@RequestMapping("/access")
public class AccessController {



    private String baseURL;

    private Integer instanceType;

    private String baseURLOne;

    private String baseURLTwo;
    private String localURL = "http://localhost:8087";

    AccessController(@Value("${application.instance.type}") Integer instanceType,
                     @Value("${api.host.baseurl.instanceOne}") String baseURLOne,
                     @Value("${api.host.baseurl.instanceTwo}") String baseURLTwo) {
        System.out.println("We are in the instance number--> " + instanceType);
        if (instanceType == 1) {
            baseURL = baseURLTwo;
        } else if (instanceType == 2) {
            baseURL = baseURLOne;
        } else {
            baseURL = localURL;
        }
//        System.out.println("Base URL is--> " + baseURL);
    }


    @GetMapping("/metadata/get")
    public List<String> greeting() {
        ArrayList<String> result = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(GLOBAL_METADATA))) {
            String content = "";
            while((content = br.readLine())!=null){
                result.add(content);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @GetMapping("/query/get")
    public String[] greeting(@RequestParam String query) {
        List <String> result = new ArrayList<>();
        TableQuery tableQuery = new TableQuery();
        result = tableQuery.selectRows(query);
        String[] stringArray = result.toArray(new String[0]);
        return stringArray;
    }

    @GetMapping("/query/insert")
    public boolean insert(@RequestParam String query) {
//        List <String> result = new ArrayList<>();
        TableQuery tableQuery = new TableQuery();
        boolean result = tableQuery.insertRow(query);
//        String[] stringArray = result.toArray(new String[0]);
        return result;
    }
}
