import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.IOException;
import java.math.BigInteger;        
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisClientUtil;

/**
 * Start a specified number of producer and consumer threads, and produce-consume
 * for the least of the specified duration and 1 hour. Some messages can be left
 * in the queue because producers and consumers might not be in exact balance.
 */
public class MultipleShardsProducerKinesis {

    // The maximum runtime of the program.
    private final static int MAX_RUNTIME_MINUTES = 60;
    private final static Log log = LogFactory.getLog(MultipleShardsProducerKinesis.class);
    private static FileWriter w;
    private static BufferedWriter writer;

    public static void main(String[] args) throws InterruptedException, IOException {

    	String streamName = "kinesis-h";
        String regionName = "eu-west-1";
        Region region = Region.of(regionName);
        if (region == null) {
            System.err.println(regionName + " is not a valid AWS region.");
            System.exit(1);
        }

        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));

        validateStream(kinesisClient, streamName);

        final Scanner input = new Scanner(System.in);
        try {
            w = new FileWriter("MyFile.txt", true);
            writer = new BufferedWriter(w);

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.print("Enter the number of producers: ");
        final int producerCount = input.nextInt();

        System.out.print("Enter the number of messages per batch: ");
        final int batchSize = input.nextInt();

        System.out.print("Enter the message size in bytes: ");
        final int messageSizeByte = input.nextInt();

        System.out.print("Enter the run time in minutes: ");
        final int runTimeMinutes = input.nextInt();

        /*
         * Create a new instance of the builder with all defaults (credentials
         * and region) set automatically. For more information, see Creating
         * Service Clients in the AWS SDK for Java Developer Guide.
         */

        // The flag used to stop producer, consumer, and monitor threads.
        final AtomicBoolean stop = new AtomicBoolean(false);

        // Start the producers.
        final AtomicInteger producedCount = new AtomicInteger();
        final Thread[] producers = new Thread[producerCount];
        for (int i = 0; i < producerCount; i++) {
            if (batchSize == 1) {
                producers[i] = new Producer(kinesisClient, messageSizeByte,
                        producedCount, stop, streamName);
            }
            producers[i].start();
        }
        // Start the monitor thread.
        final Thread monitor = new Monitor(producedCount, stop);
        monitor.start();

        // Wait for the specified amount of time then stop.
        Thread.sleep(TimeUnit.MINUTES.toMillis(Math.min(runTimeMinutes,
                MAX_RUNTIME_MINUTES)));
        stop.set(true);

        // Join all threads.
        for (int i = 0; i < producerCount; i++) {
            producers[i].join();
        }

        monitor.interrupt();
        monitor.join();
        writer.flush();
        writer.close();
    }

    private static String makeRandomString(int sizeByte) throws IOException {
        final byte[] bs = new byte[(int) Math.ceil(sizeByte * 5 / 8)];
        new Random().nextBytes(bs);
        bs[0] = (byte) ((bs[0] | 64) & 127);
        String i = new BigInteger(bs).toString(32);

        return i;
    }

    /**
     * The producer thread uses {@code SendMessage}
     * to send messages until it is stopped.
     */
    private static class Producer extends Thread {
        final KinesisAsyncClient kinesisClient;
        final AtomicInteger producedCount;
        final AtomicBoolean stop;
        final String theMessage;
        final String streamName;

        Producer(KinesisAsyncClient kinesisClient, int messageSizeByte,
                 AtomicInteger producedCount, AtomicBoolean stop, String streamName) throws IOException {
            this.kinesisClient = kinesisClient;
            this.producedCount = producedCount;
            this.stop = stop;
            this.theMessage = makeRandomString(messageSizeByte);
            this.streamName = streamName;
        }

        public void run() {
            try {
                while (!stop.get()) {
                    PutRecordRequest request = PutRecordRequest.builder()
                            .partitionKey(1) // We use the ticker symbol as the partition key, explained in the Supplemental Information section below.
                            .streamName(streamName)
                            .data(SdkBytes.fromByteArray(theMessage.getBytes()))
                            .build();
                    try {
                        kinesisClient.putRecord(request).get();
                    } catch (InterruptedException e) {
                        LOG.info("Interrupted, assuming shutdown.");
                    } catch (ExecutionException e) {
                        LOG.error("Exception while sending data to Kinesis. Will try again next cycle.", e);
                    }

                }
            } catch (AmazonClientException | IOException e) {
                /*
                 * By default, AmazonSQSClient retries calls 3 times before
                 * failing. If this unlikely condition occurs, stop.
                 */
                log.error("Producer: " + e.getMessage());
                System.exit(1);
            }
        }
    }

    /**
     * The producer thread uses {@code SendMessageBatch}
     * to send messages until it is stopped.
     */


    private static class Monitor extends Thread {
        private final AtomicInteger producedCount;
        private final AtomicBoolean stop;

        Monitor(AtomicInteger producedCount,
                AtomicBoolean stop) {
            this.producedCount = producedCount;
            this.stop = stop;
        }

        public void run() {
            try {
                while (!stop.get()) {
                    Thread.sleep(1000);
                    log.info("produced messages = " + producedCount.get() );
                }
            } catch (InterruptedException e) {
                // Allow the thread to exit.
            }
        }
    }
}
