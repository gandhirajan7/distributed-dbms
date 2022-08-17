package com.dpgten.distributeddb.test;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicLong;

@RestController
public class TestController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    @GetMapping("/test/get")
    public String greeting(@RequestParam(value = "name", defaultValue = "World") String name) {
        return new String(counter.incrementAndGet() + String.format(template, name));
    }
}

