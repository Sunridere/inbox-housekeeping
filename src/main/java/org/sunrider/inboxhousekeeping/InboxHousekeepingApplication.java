package org.sunrider.inboxhousekeeping;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class InboxHousekeepingApplication {

    public static void main(String[] args) {
        SpringApplication.run(InboxHousekeepingApplication.class, args);
    }

}
