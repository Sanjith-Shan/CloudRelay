package com.cloudrelay;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Context load test. Requires Redis on localhost 6379 and MongoDB on
 * localhost 27017, matching the services defined in docker compose and CI.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CloudRelayApplicationTests {

    @Test
    void contextLoads() {
    }
}
