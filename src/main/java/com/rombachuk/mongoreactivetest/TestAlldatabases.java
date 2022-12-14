package com.rombachuk.mongoreactivetest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;

public class TestAlldatabases {

    private List<ListDatabasesPublisher<Document>> publishers = new ArrayList<ListDatabasesPublisher<Document>>();
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

    public  TestAlldatabases (Map<String, String> configuration, Connection connection) {

        List<String> expected = Arrays.asList("enable", "publishers", "subscribers_per_publisher");

        if (valid_configuration (configuration,expected)) {
                enabled = true;
                Integer num_publishers =  Integer.parseInt(configuration.get("publishers").toString());
                for (Integer i = 0; i < num_publishers; i++) {
                    publishers.add(connection.mongoClient.listDatabases());
                }
        }        
        this.configuration = configuration;   
    }

    public  void run (Integer loop) {

        Integer num_publishers =  Integer.parseInt(configuration.get("publishers").toString());
        Integer subs_per_pub =  Integer.parseInt(this.configuration.get("subscribers_per_publisher").toString());

        Integer pindex = 0;
        for (ListDatabasesPublisher<Document> publisher : this.publishers) {
            Integer sindex = 0;
            for (Integer j=0; j < subs_per_pub; j++) {
                String logprefix = "Loop ["+loop.toString()+"] alldatabases Pubsub [" + loop.toString()+"/"+ formatInt(pindex, num_publishers) + 
                "/"+ formatInt(sindex, subs_per_pub)+"] ";
            publisher.subscribe(new SubscriberHelpers.LogDocumentSubscriber(logprefix)); 
            sindex = sindex + 1;
            }
            pindex = pindex + 1;
        }

    }
    
}
