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

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomConnectionPoolListener implements ConnectionPoolListener {

    // Use thread-safe atomicInteger to get correct counts
    //
    private static Logger logger = LoggerFactory.getLogger(App.class.getName());
    private AtomicInteger checkout_success = new AtomicInteger( 0 );
    private AtomicInteger checkout_fail = new AtomicInteger( 0 );
    private AtomicInteger checkout_count = new AtomicInteger( 0 );
    private AtomicInteger connection_success = new AtomicInteger( 0 );
    private AtomicInteger connection_count = new AtomicInteger( 0 );
    private AtomicInteger connection_ready = new AtomicInteger( 0 );
    
    private String formatCounts() {
       String connection_result =  "{connections->[current:"+connection_ready.toString() + "/success:"+connection_success.toString()+
        "/attempts:"+connection_count.toString()+"]}";
       String checkout_result =  "{checkouts->[current:"+checkout_count.toString() + "/success:"+checkout_success.toString()+
        "/fail:"+checkout_fail.toString()+"]}";
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
        this.checkout_count.incrementAndGet();
        this.checkout_success.incrementAndGet();
        logger.info("ConnectionPoolListener [checkedout]: "+formatCounts());

    }

    @Override
    public void connectionCheckOutFailed(ConnectionCheckOutFailedEvent event) {
        this.checkout_fail.incrementAndGet();
        logger.info("ConnectionPoolListener [checkoutfail]: "+formatCounts());
        logger.info("ConnectionPoolListener [checkoutfail]: "+event);
    }

    @Override
    public void connectionCheckedIn(final ConnectionCheckedInEvent event) {     
        this.checkout_count.decrementAndGet();
        logger.info("ConnectionPoolListener [checkedin]: "+formatCounts());
    }

    @Override
    public void connectionCreated(ConnectionCreatedEvent event) {
        this.connection_count.incrementAndGet();
        logger.info("ConnectionPoolListener [conncreate]: "+formatCounts());

    }

    @Override
    public void connectionReady(ConnectionReadyEvent event) {
        this.connection_ready.incrementAndGet();
        this.connection_success.incrementAndGet();
        logger.info("ConnectionPoolListener [connready]: "+formatCounts());
    }

    @Override
    public void connectionClosed(ConnectionClosedEvent event) {
        this.connection_ready.decrementAndGet();
        logger.info("ConnectionPoolListener [connclosed]: "+formatCounts());
    }


}
