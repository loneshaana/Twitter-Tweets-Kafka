package tutorial1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
    public ConsumerDemoWithThreads(){}

    public static void main(String[] args) throws InterruptedException {
        new ConsumerDemoWithThreads().run();
    }

    public void run() throws InterruptedException {
        logger.info("Creating the consumer thread");
        String bootStrapServers = "localhost:9092";
        String groupIp = "my_fourth_application";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerThread(latch, topic, bootStrapServers, groupIp);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread( () ->{
                logger.info("Caught shutdown hook");
                ((ConsumerThread) myConsumerRunnable).shutdown();
                try {
                    latch.await();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                logger.info("Application has ended");
            }));

        try {
            latch.await();
        }catch (InterruptedException e){
            logger.error("Application Got Interrupted "+e);
        }finally {
            logger.info("Application Is Closing");
        }
    }
}


