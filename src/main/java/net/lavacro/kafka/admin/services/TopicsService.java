package net.lavacro.kafka.admin.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
@Service
public class TopicsService {
	private final AdminClient adminClient;

	public void topics() {
		log.info("Getting topics");
		ListTopicsResult result = adminClient.listTopics();
		try  {
			List<String> topics = new ArrayList<>(result.names().get().stream().toList());
			Collections.sort(topics);
			topics.forEach(topic -> log.info("Topic: {}", topic));
		} catch(Exception e) {
			log.error(e.getMessage());
		}
	}
}
