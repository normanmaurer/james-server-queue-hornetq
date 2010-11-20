package me.norman.maurer.james.queue;

import org.apache.james.queue.api.MailQueue;
import org.apache.james.queue.jms.JMSMailQueueFactory;

public class HornetQMailQueueFactory extends JMSMailQueueFactory{

    @Override
    protected MailQueue createMailQueue(String name, boolean useJMX) {
        MailQueue queue = new HornetQMailQueue(connectionFactory, name, log);
        if (useJMX) {
            registerMBean(name, queue);
        }
        return queue;
    }

}
