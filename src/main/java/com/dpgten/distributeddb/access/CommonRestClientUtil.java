package com.dpgten.distributeddb.access;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.*;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.HashMap;
import java.util.Map;

@Component
public class CommonRestClientUtil<T, K> {

    @Autowired
    private RestTemplate restTemplate;

    /**
     * Common rest call.
     *
     * @param <T>          the generic type
     * @param <K>          the key type
     * @param url          the url
     * @param httpMethod   the http method
     * @param request      the request
     * @param responseType the response type
     * @param pathParams   the path params
     * @param queryParams  the query params
     * @return the t
     */
    @Retryable(value = {HttpServerErrorException.class, ResourceAccessException.class,
            HttpClientErrorException.class}, maxAttempts = 3, backoff = @Backoff(delay = 2000))
    public <T, K> T commonRestCall(String url, HttpMethod httpMethod, K request, Class<T> responseType,
                                   Map<String, String> pathParams, Map<String, String> queryParams) {
        T response = null;
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            if (null == pathParams)
                pathParams = new HashMap<>();
            if (null == queryParams)
                queryParams = new HashMap<>();
            HttpEntity<Object> requestEntity = new HttpEntity<>(request, headers);
            System.out.println("Path Params:: {" + pathParams + "}");
            System.out.println("Query Params:: {" + queryParams + "}");
            MultiValueMap<String, String> qParams = new LinkedMultiValueMap<>();
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                qParams.add(entry.getKey(), entry.getValue());
            }
            String requestUrl = UriComponentsBuilder.fromHttpUrl(url).queryParams(qParams).buildAndExpand(pathParams)
                    .toUriString();
            System.out.println("requestUrl ::::" + requestUrl);
            ResponseEntity<T> restResponse = restTemplate.exchange(requestUrl, httpMethod, requestEntity, responseType);
            response = restResponse.getBody();
            System.out.println("Response is present");
        } catch (HttpServerErrorException httpSerErrEx) {
            httpSerErrEx.printStackTrace();
            System.out.println("HttpServerError " + httpSerErrEx);
        } catch (ResourceAccessException resourceAccEx) {
            resourceAccEx.printStackTrace();
            System.out.println("ResourceAccessError " + resourceAccEx);
        } catch (RestClientException restClientEx) {
            System.out.println("RestClientError " + restClientEx);
        } catch (Exception e) {
            System.out.println("Error in Common Rest client call" + e);
            //throw new OrchestrationCatalogueException("Exception  in Orchestration Common Rest client call");
            e.printStackTrace();
        }
        return response;
    }

}
