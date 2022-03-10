package org.mule.extension.jsonlogger.internal.destinations;

import org.mule.extension.jsonlogger.internal.destinations.Destination;
import org.mule.runtime.extension.api.annotation.param.NullSafe;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Example;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class KinesisDestination implements Destination {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisDestination.class);

    private final int MAX_CONCURRENCY = 100;
    private final int MAX_PENDING_CONNECTION_ACQUIRES = 10_000;
    
    @Parameter
    @Summary("Access key provided by Amazon")
    @DisplayName("Access Key")
    private String accessKey;

    @Parameter
    @DisplayName("Secret Key")
    @Summary("Secret key provided by Amazon")
    private String secretKey;

    @Parameter
    @DisplayName("Stream Name")
    @Summary("The stream name")
    private String streamName;

    @Parameter
    @DisplayName("Region Endpoint")
    @Example("us-east-1")
    @Optional(defaultValue = "eu-central-1")
    @Summary("Topic region endpoint")
    private String region;

    @Parameter
    @Optional
    @NullSafe
    @Summary("Indicate which log categories should be send (e.g. [\"my.category\",\"another.category\"]). If empty, all will be send.")
    @DisplayName("Log Categories")
    private ArrayList<String> logCategories;

    private KinesisAsyncClient kinesisClient;

    @Override
    public String getSelectedDestinationType() {
        return "Kinesis";
    }

    @Override
    public ArrayList<String> getSupportedCategories() {
        return logCategories;
    }

    @Override
    public int getMaxBatchSize() {
        return 1;
    }

    @Override
    public void sendToExternalDestination(String finalLog) {
        LOGGER.info("Data being sent to AWS Kinesis Stream (Async) {}", new String(finalLog.getBytes(), StandardCharsets.UTF_8));
        PutRecordRequest request = PutRecordRequest.builder()
                                                   .partitionKey(UUID.randomUUID().toString())
                                                   .streamName(this.streamName)
                                                   .data(SdkBytes.fromUtf8String(finalLog)).build();

        try {
            CompletableFuture<PutRecordResponse> putRecordResponse = kinesisClient.putRecord(request);
            PutRecordResponse response = putRecordResponse.get();
            LOGGER.info("Data Sent to AWS Kinesis Stream (Async). AWS Result {}", response);
        } catch (KinesisException e) {
            e.getMessage();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initialise() {
        this.kinesisClient = KinesisAsyncClient.builder()
                .region(Region.of(this.region))
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(this.accessKey, this.secretKey)))
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .maxConcurrency(MAX_CONCURRENCY)
                        .maxPendingConnectionAcquires(MAX_PENDING_CONNECTION_ACQUIRES))
                .build();
    }

    @Override
    public void dispose() {
        this.kinesisClient.close();
    }
}