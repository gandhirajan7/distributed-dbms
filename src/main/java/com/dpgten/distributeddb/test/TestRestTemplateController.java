package com.dpgten.distributeddb.test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController(value = "/rest")
public class TestRestTemplateController {

    @Autowired
    RestTemplate restTemplate;
    private String baseURL;
//    @Value("${application.instance.type}")
    private Integer instanceType;
//    @Value("${api.host.baseurl.instanceOne}")
    private String baseURLOne;
//    @Value("${api.host.baseurl.instanceTwo}")
    private String baseURLTwo;
    private String localURL = "http://localhost:8087";

    TestRestTemplateController(@Value("${application.instance.type}") Integer instanceType,
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

//    @RequestMapping(value = "/template/products", method = RequestMethod.POST)
//    public String createProducts(@RequestBody Product product) {
//        HttpHeaders headers = new HttpHeaders();
//        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
//        HttpEntity<Product> entity = new HttpEntity<Product>(product, headers);
//
//        return restTemplate.exchange(
//                "http://localhost:8080/products", HttpMethod.POST, entity, String.class).getBody();
//    }

    @RequestMapping(value = "/template/get", method = RequestMethod.GET)
    public String getProducts() {
//        RestTemplate restTemplate = new RestTemplate();
        String fooResourceUrl = baseURL + "/test/get";
        ResponseEntity<String> response
                = restTemplate.getForEntity(fooResourceUrl, String.class);
//    Assertions.assertEquals(response.getStatusCode(), HttpStatus.OK);
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode root = null;
//        String result = "";
        String responseStr = "";
        try {
            responseStr = response.getBody();
            System.out.println(responseStr);
//            root = mapper.readTree(response.getBody());
//            result = root.toString();
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        JsonNode name = root.path("name");
        return responseStr;
    }


}
