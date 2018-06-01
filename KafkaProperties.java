package com.instaclustr.kongokafkastreams.blog;

public class KafkaProperties {
	public static final Boolean DEBUG = false;
	public static final Boolean DISPLAY = true;
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;
    public static final int DELAY = 100;
    public static final String TOPIC = "kongo_topic1";
    public static final String CLIENT_ID = "KongoApplication";

    private KafkaProperties() {}
}