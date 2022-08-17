package com.dpgten.distributeddb.access;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.ArrayList;
import java.util.List;

@Component
public class RestCallController {


    RestTemplate restTemplate = new RestTemplate();

    private String baseURL;

    private String baseURLOne;

    private String baseURLTwo;

    private String localURL = "http://localhost:8087/";

//    public RestCallController(@Value("${application.instance.type}") Integer instanceType,
//                       @Value("${api.host.baseurl.instanceOne}") String baseURLOne,
//                       @Value("${api.host.baseurl.instanceTwo}") String baseURLTwo)
    public RestCallController(){
//        System.out.println("We are in the instance number--> " + instanceType);
        this.baseURLOne = baseURLOne;
        this.baseURLTwo = baseURLTwo;
//        if (instanceType == 1) {
//            baseURL = baseURLTwo;
//        } else if (instanceType == 2) {
//            baseURL = baseURLOne;
//        } else {
//            baseURL = localURL;
//        }
//        System.out.println("Base URL is--> " + baseURL);
    }

    public String[] selectRestCall(String query, String vmOption) {
//        if (vmOption == 1) {
//            this.baseURL = this.baseURLOne;
//        } else {
//            this.baseURL = this.baseURLTwo;
//        }
        String resourceUrl = "http://"+ vmOption + ":8087/access/query/get";
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(resourceUrl)
                .queryParam("query", query);

        HttpEntity<String> requestEntity = new HttpEntity<>(null, null);
        ResponseEntity<String[]> response = restTemplate.exchange(builder.buildAndExpand().toUri(),HttpMethod.GET, requestEntity , String[].class);
//                =restTemplate.getForEntity(fooResourceUrl, List.class);
//        List<String> responseStr = new ArrayList<>();
        String [] responseString = null;
        try {
            responseString = response.getBody();
//            ObjectMapper mapper = new ObjectMapper();
//            responseStr = mapper.readTree();
//            JsonNode name = root.path("");
//            Assertions.assertNotNull(name.asText());
//            responseStr = response.getBody();
//            System.out.println(responseStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return responseString;
    }


    public boolean insertRestCall(String query, String vmOption) {

        String resourceUrl = "http://"+ vmOption + ":8087/access/query/insert";
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(resourceUrl)
                .queryParam("query", query);

        HttpEntity<String> requestEntity = new HttpEntity<>(null, null);
        ResponseEntity<Boolean> response = restTemplate.exchange(builder.buildAndExpand().toUri(),HttpMethod.GET, requestEntity , boolean.class);

        boolean responseString = false;
        try {
             responseString = response.getBody();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return responseString;
    }
}
