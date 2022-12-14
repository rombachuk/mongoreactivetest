package com.rombachuk.mongoreactivetest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import com.mongodb.reactivestreams.client.MongoDatabase;

public class TestAllcollections {

    private List<MongoDatabase> publishers = new ArrayList<MongoDatabase>();
    private Map<String, String> configuration;
    private Boolean enabled = false;

    public Boolean getEnabled () {
        return enabled;
    }

    private static Boolean valid_configuration (Map<String, String> configuration, List<String> expected) {

        Boolean result = true; 
        if (Integer.compare(configuration.size(),expected.size()) != 0) {
            result = false;
        } else {
            for ( String ekey : expected) {
                if (!configuration.containsKey(ekey)) {
                    result = false;
                    break;
                }
            }
            if (!configuration.get("enable").equals("true")) {
               result = false;
            }
        }  
        return result;
    }    

    private String formatInt (int num, int max) {  
        if (max >= 10000) {
            return String.format("%05d",num);
        }  else if (max >= 1000) {
            return String.format("%04d",num);
        }  else if (max >= 100) {
            return String.format("%03d",num);
        }  else {
            return String.format("%02d",num);
        }
    }


    public  TestAllcollections (Map<String, String> configuration, Connection connection) {
       
        List<String> expected = Arrays.asList("enable", "publishers", "subscribers_per_publisher","database");
        if (valid_configuration (configuration,expected)) {
                    enabled = true;
                    int num_publishers =  Integer.parseInt(configuration.get("publishers").toString());
                    String testdatabase =  configuration.get("database").toString();
                    for (int i = 0; i < num_publishers; i++) {
                        publishers.add(connection.mongoClient.getDatabase(testdatabase));
                    }
        }
        this.configuration = configuration;
    }

    public  void run (Integer loop) {

        int num_publishers =  Integer.parseInt(configuration.get("publishers").toString());
        int subs_per_pub =  Integer.parseInt(this.configuration.get("subscribers_per_publisher").toString());

        int pindex = 0;
        for (MongoDatabase publisher : this.publishers) {
            int sindex = 0;
            for (int j=0; j < subs_per_pub; j++) {
            String logprefix = "Loop ["+loop.toString()+"] allcollections Pubsub [" + loop.toString()+"/"+ formatInt(pindex, num_publishers) + 
            "/"+ formatInt(sindex, subs_per_pub)+"] ";
            publisher.listCollectionNames().subscribe(new SubscriberHelpers.LogToStringSubscriber<String>(logprefix)); 
            sindex = sindex + 1;
            }
            pindex = pindex + 1;
        }

    }
    
}
