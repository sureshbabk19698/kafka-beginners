package org.sk.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.messaging.Message;

@Getter
@Setter
public class MessageWrapper {

    private String status = "FAILED";
    private Message<String> message;

}
