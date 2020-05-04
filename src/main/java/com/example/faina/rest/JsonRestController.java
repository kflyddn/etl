package com.example.faina.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping(value = "/api", method = RequestMethod.POST)
public class JsonRestController {

    //TODO: create an error page for /error
    //This application has no explicit mapping for /error, so you are seeing this as a fallback.

    private static Logger logger = LoggerFactory.getLogger(JsonRestController.class);

    @RequestMapping(value = "/json", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
    public Map<String, Object> postJson(@RequestBody Map<String, Object> payload)   {
        logger.info("rest controller got a message:\n"+payload);
        //TODO: persist json to ES

        return payload;
    }
}
