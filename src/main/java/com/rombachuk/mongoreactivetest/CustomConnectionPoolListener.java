package com.rombachuk.mongoreactivetest;


import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionReadyEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomConnectionPoolListener implements ConnectionPoolListener {

    private static Logger logger = LoggerFactory.getLogger(App.class.getName());
    private int checkout_success = 0;
    private int checkout_fail = 0;
    private int checkout_count = 0;
    private int connection_success = 0;
    private int connection_count = 0;
    private int connection_ready = 0;

    private String formatCounts() {
       String connection_result =  "{connections->[current:"+Integer.toString(connection_ready) + "/success:"+Integer.toString(connection_success)+
        "/attempts:"+Integer.toString(connection_count)+"]}";
       String checkout_result =  "{checkouts->[current:"+Integer.toString(checkout_count) + "/success:"+Integer.toString(checkout_success)+
        "/fail:"+Integer.toString(checkout_fail)+"]}";
       String result = connection_result.concat(checkout_result);
       return result;
    }

    @Override
    public void connectionPoolCreated(ConnectionPoolCreatedEvent event){
        logger.info("ConnectionPoolListener [poolcreated]: "+formatCounts());
    }

    @Override
    public void connectionPoolClosed(final ConnectionPoolClosedEvent event) {
        logger.info("ConnectionPoolListener [poolclosed]: "+formatCounts());
    }

    @Override
    public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
        this.checkout_count = this.checkout_count + 1;
        this.checkout_success = this.checkout_success + 1;
        logger.info("ConnectionPoolListener [checkedout]: "+formatCounts());

    }

    @Override
    public void connectionCheckOutFailed(ConnectionCheckOutFailedEvent event) {
        this.checkout_fail = this.checkout_fail + 1;
        logger.info("ConnectionPoolListener [checkoutfail]: "+formatCounts());
        logger.info("ConnectionPoolListener [checkoutfail]: "+event);
    }

    @Override
    public void connectionCheckedIn(final ConnectionCheckedInEvent event) {     
        this.checkout_count = this.checkout_count - 1;
        logger.info("ConnectionPoolListener [checkedin]: "+formatCounts());
    }

    @Override
    public void connectionCreated(ConnectionCreatedEvent event) {
        this.connection_count = this.connection_count + 1;
        logger.info("ConnectionPoolListener [conncreate]: "+formatCounts());

    }

    @Override
    public void connectionReady(ConnectionReadyEvent event) {
        this.connection_ready = this.connection_ready + 1;
        this.connection_success = this.connection_success + 1;
        logger.info("ConnectionPoolListener [connready]: "+formatCounts());
    }

    @Override
    public void connectionClosed(ConnectionClosedEvent event) {
        this.connection_ready = this.connection_ready - 1;
        logger.info("ConnectionPoolListener [connclosed]: "+formatCounts());
    }


}
