package org.sk.config;

public class KafkaTopic {

    public static final String MULTI_PARTITION_TOPIC = "multi_partition_topic";
    public static final String SINGLE_PARTITION_TOPIC = "single_partition_topic";
    public static final String SINGLE_FILTER_PARTITION_TOPIC = "single_filter_partition_topic";

    public static final String SOURCE_DLT_TOPIC = "source_dlt_topic";
    public static final String RECOVER_DLT_TOPIC = "recover_dlt_topic"; // dead letter topic where records will be published to, if source topic is failed processing data

}

