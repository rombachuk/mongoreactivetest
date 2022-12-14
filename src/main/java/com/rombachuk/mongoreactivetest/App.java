package com.rombachuk.mongoreactivetest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class App {
    private App() {

    }

    private static Logger logger = LoggerFactory.getLogger(App.class.getName());

    public static void main(String[] args)  {

        System.setProperty("java.util.logging.SimpleFormatter.format",
              "[%1$tF %1$tT] [%4$-7s] %5$s %n");

        final String inifilename = args[0];
        Configuration configuration = null;
        try {
        configuration = new Configuration(inifilename);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
        logger.info("Reading truststore info");
        String truststore_location = configuration.map.get("truststore").get("location").toString();
        String obftruststore_password = configuration.map.get("truststore").get("password").toString();
        byte[] decodedBytes = Base64.getDecoder().decode(obftruststore_password);
        String truststore_password = new String(decodedBytes);

        logger.info("Reading activity spec");
        Integer loop_idle_time = Integer.parseInt(configuration.map.get("activity").get("loop_idle_time").toString());
        Integer loops = Integer.parseInt(configuration.map.get("activity").get("loops").toString());
       
        System.setProperty("javax.net.ssl.trustStore",truststore_location);
        System.setProperty("javax.net.ssl.trustStorePassword", truststore_password);

        logger.info("Making connection");

        final Connection connection = new Connection(configuration);
        final int poolaccesstimeout = connection.getPoolaccesstimeout();
        final TestAlldatabases testalldatabases = new TestAlldatabases(configuration.map.get("test-alldatabases"), connection);
        final TestAllcollections testallcollections = new TestAllcollections(configuration.map.get("test-allcollections"), connection);

        logger.info("Running activity loops");
        logger.info("test-alldatabases enabled="+testalldatabases.getEnabled());
        logger.info("test-allcollections enabled="+testallcollections.getEnabled());

        for (Integer i = 0; i < loops; i++) {      
            try { 
                
            if (testalldatabases.getEnabled()) {
                testalldatabases.run(i);
            }
            if (testallcollections.getEnabled()) {
                testallcollections.run(i);
            }
                logger.info("Loop ["+i+"] Sleeping for ["+loop_idle_time+"] seconds...");
                TimeUnit.SECONDS.sleep(loop_idle_time);
                
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                logger.info("Interrupted Exception");
                e.printStackTrace();
            }
            catch (com.mongodb.MongoInterruptedException e) {
                // TODO Auto-generated catch block
                logger.info("Interrupted Exception");
                e.printStackTrace();
            }

        }

        if (loop_idle_time <= poolaccesstimeout) {
            try {
                logger.info("Loop-time < pool-access-timeout of ["+
                             Integer.toString(poolaccesstimeout)+"s]: Running final wait to allow async pool to timeout");
                TimeUnit.SECONDS.sleep(poolaccesstimeout);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                logger.info("Interrupted Exception during sleep");
                e.printStackTrace();
            }           
        }
        connection.mongoClient.close();

        logger.info("Completed activity");
    }
}
