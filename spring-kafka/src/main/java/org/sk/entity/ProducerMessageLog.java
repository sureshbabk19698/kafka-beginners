package org.sk.entity;

import jakarta.persistence.*;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;


@EqualsAndHashCode(callSuper = false)
@Entity
@Table(name = "PRODUCER_MSG_LOG")
@Data
@Builder
public class ProducerMessageLog extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "LOG_ID")
    private long logId;

    @Column(name = "CORRELATION_ID")
    private String correlationId;

    @Column(name = "MSG_SENT_TS")
    private Date messagesSentTs;

    @Column(name = "TOPIC")
    private String topic;

    @Column(name = "PARTITION_ID")
    private Integer partitionId;

    @Column(name = "PAYLOAD")
    private String payload;

    @Column(name = "ERROR_MSG")
    private String errorMessage;

    @Column(name = "PROCESS_STATUS")
    private String processStatus;

}
