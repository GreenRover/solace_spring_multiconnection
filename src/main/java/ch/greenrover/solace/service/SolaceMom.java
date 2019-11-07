package ch.greenrover.solace.service;

import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLContentMessage;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Scope("singleton")
@Slf4j(topic = "demo_app")
@RequiredArgsConstructor
public class SolaceMom {
	private JCSMPSession session;

	private final SpringJCSMPFactory factory;

	private XMLMessageProducer producer;

	@PostConstruct
	private void init() throws JCSMPException, UnknownHostException {
		session = factory.createSession();
		System.out.println("create session");
		
		producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {

			@Override
			public void handleError(final String messageID, final JCSMPException e, final long timestamp) {
				log.error(String.format("Producer received error for msg: %s@%s - %s", messageID, timestamp, e));
			}

			@Override
			public void responseReceived(final String messageID) {
			}
		});

	}
	
	public void consumeTopicsWithQueue(final String queueName, final Collection<String> topics, final Consumer<BytesXMLMessage> consumer) throws JCSMPException {
		final Queue queue = createQueue(session, queueName);
		consumeTopicsWithQueue(queue, topics, consumer);
	}
		

	public void consumeTopicsWithQueue(final Queue queue, final Collection<String> topics, final Consumer<BytesXMLMessage> consumer) throws JCSMPException {
		
		// Bind topics to queue
		for (final String topicName: topics) {
			try {
				session.addSubscription(queue, JCSMPFactory.onlyInstance().createTopic(topicName), JCSMPSession.WAIT_FOR_CONFIRM);
			} catch (final JCSMPErrorResponseException e) {
				if (JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT != e.getSubcodeEx()) {
	                throw new RuntimeException(e);
	            }
			}
		}
		

		// Create a Flow be able to bind to and consume messages from the Queue.
		final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
		flow_prop.setEndpoint(queue);
		flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);

		final EndpointProperties endpoint_props = new EndpointProperties();
		endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

		final FlowReceiver cons = session.createFlow(new XMLMessageListener() {
			@Override
			public void onReceive(final BytesXMLMessage msg) {
				consumer.accept(msg);
			}

			@Override
			public void onException(final JCSMPException e) {
				log.error("Consumer received exception: " + e);
			}
		}, flow_prop, endpoint_props);

		log.info("Start consuming queue: " + queue.getName());
		cons.start();
	}

	private static Queue createQueue(final JCSMPSession session, final String queueName) throws JCSMPException {
		log.debug("Attempting to provision the queue '%s' on the appliance.%n", queueName);

		final EndpointProperties endpointProps = new EndpointProperties();
		// set queue permissions to "consume" and access-type to "exclusive"
		endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
		endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
		// create the queue object locally
		final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		// Actually provision it, and do not fail if it already exists
		session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);

		return JCSMPFactory.onlyInstance().createQueue(queueName);
	}
	
	public void sendReply(final XMLMessage requestMessage, final XMLMessage replyMessage) throws JCSMPException {
		producer.sendReply(requestMessage, replyMessage);
	}
	
	public void send(final XMLMessage requestMessage, final Destination destination) throws JCSMPException {
		requestMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
		producer.send(requestMessage, destination);
	}
	
	public TextMessage createMessage() {
		return JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
	}

	public Queue createQueue(String queueName) throws JCSMPException {
		return createQueue(session, queueName);
	}
	
	public Topic createTopic(String topicName) {
		return JCSMPFactory.onlyInstance().createTopic(topicName);
	}
	
	public static String getTextFromMsg(final BytesXMLMessage msg) {
		if (msg instanceof TextMessage) {
			return ((TextMessage) msg).getText();
		} else if (msg instanceof XMLContentMessage) {
			return ((XMLContentMessage)msg).getXMLContent();
		} else {
			return StandardCharsets.UTF_8.decode(msg.getAttachmentByteBuffer()).toString();
		}
	}
}
