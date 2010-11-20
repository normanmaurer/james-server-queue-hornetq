/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/
package me.norman.maurer.james.queue;

/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/
import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.mail.MessagingException;

import org.apache.commons.logging.Log;
import org.apache.james.core.MimeMessageCopyOnWriteProxy;
import org.apache.james.core.MimeMessageInputStream;
import org.apache.james.core.MimeMessageInputStreamSource;
import org.apache.james.core.MimeMessageWrapper;
import org.apache.james.queue.jms.JMSMailQueue;
import org.apache.mailet.Mail;

public class HornetQMailQueue extends JMSMailQueue {

    public HornetQMailQueue(ConnectionFactory connectionFactory, String queuename, Log logger) {
        super(connectionFactory, queuename, logger);
    }

    @Override
    protected void produceMail(Session session, Map<String, Object> props, int msgPrio, Mail mail) throws JMSException, MessagingException, IOException {
        MessageProducer producer = null;
        try {
            BytesMessage message = session.createBytesMessage();
            Iterator<String> propIt = props.keySet().iterator();
            while (propIt.hasNext()) {
                String key = propIt.next();
                message.setObjectProperty(key, props.get(key));
            }
            // set the stream to read frome
            message.setObjectProperty("JMS_HQ_InputStream", new BufferedInputStream(new MimeMessageInputStream(mail.getMessage())));
            Queue q = session.createQueue(queuename);
            producer = session.createProducer(q);
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, msgPrio, Message.DEFAULT_TIME_TO_LIVE);
        } finally {
            if (producer != null) {
                producer.close();
            }
        }
    }

    @Override
    protected void populateMailMimeMessage(Message message, Mail mail) throws MessagingException, JMSException {
        BytesMessage b = (BytesMessage) message;
       
        // as HornetQ can read from the stream via a BytesMessage just do it
        mail.setMessage(new MimeMessageCopyOnWriteProxy(new MimeMessageWrapper(new MimeMessageInputStreamSource(message.getJMSMessageID().replaceAll(":", "_"), new BytesMessageInputStream(b)))));
    }

}
