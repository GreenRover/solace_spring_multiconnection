package ch.greenrover.solace.controler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.TextMessage;

import ch.greenrover.solace.service.SolaceMom;
import lombok.extern.slf4j.Slf4j;

@Slf4j(topic = "demo_app")
@Component
public class TestControler implements CommandLineRunner {

	@Autowired
	private SolaceMom solaceMom;

	@Autowired
	private TaskExecutor taskExecutor;

	@Value("${solace.java.topic.applicationStartup}")
	private String applicationStartupDest;

	@Value("${solace.java.topic.globalControl}")
	private String globalControlDest;

	@Value("${solace.java.topic.applicationControl}")
	private String applicationControlDest;

	@Value("${solace.java.queue.inbox}")
	private String applicationInboxQueue;

	@Override
	public void run(final String... args) throws Exception {

		taskExecutor.execute(() -> {
			startListen();

			do { // Prevent spring to be quited.
				try {
					try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in, "UTF-8"))) {
						while (true) {
							final String line = input.readLine().trim();

							switch (line.charAt(0)) {
							case 's':
							case 'S':
								System.out.println("Send ServiceDescription caused by keyboard input");
								sendLoremIpsum();
								break;
							}

						}
					}

				} catch (final Exception e) {
					log.error("Problem read from keyboard", e);
				} 
			} while (true);
		});

		// Send service Desc at startup.
		try {
			sendLoremIpsum();
		} catch (final Exception e) {
			log.error("Unable to send serviceDesc", e);
			System.exit(1);
		}
	}

	public void sendLoremIpsum() throws JCSMPException {
		final TextMessage msg = solaceMom.createMessage();
		msg.setText("Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam ");
		msg.setReplyTo(solaceMom.createQueue(applicationInboxQueue));

		solaceMom.send( //
				msg, //
				solaceMom.createTopic(applicationStartupDest) //
		);
	}

	public void startListen() {
		final List<String> topics = Arrays.asList(globalControlDest, applicationControlDest);
		try {
			solaceMom.consumeTopicsWithQueue(applicationInboxQueue, topics, msg -> {
				final String msgBody = SolaceMom.getTextFromMsg(msg);
				try {
					System.out.println(msgBody);
				} catch (final Exception e) {
					log.error("Unable to parse reply:\n" + msgBody, e);
				}
			});
		} catch (final JCSMPException e) {
			log.error("Unable to listen on inbox", e);
		}
	}
}
