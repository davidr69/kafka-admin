package net.lavacro.kafka.admin.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
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
			topics.forEach(topic -> {
				log.info("Topic: {}", topic);
				partitions(topic);
			});
		} catch(Exception e) {
			log.error(e.getMessage());
		}
	}

	public void clusters() {
		log.info("Getting clusters");
		DescribeClusterResult result = adminClient.describeCluster();
		try {
			log.info("Nodes ...");
			result.nodes().get().forEach(node -> log.info("Node: {}", node));
			log.info("Cluster ID: {}", result.clusterId().get());
			log.info("Controller: {}", result.controller().get());
		} catch(Exception e) {
			log.error(e.getMessage());
		}
	}

	public void partitions(String topic) {
		log.info("Getting partitions");
		DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topic));
		try {
			log.info("Partitions ...");
			result.topicNameValues().forEach((name, desc) -> {
				log.info("Topic: {}", name);
				try {
					desc.get().partitions().forEach(partition -> log.info("Partition: {}", partition));
				} catch(Exception e) {
					log.error(e.getMessage());
				}
			});
		} catch(Exception e) {
			log.error(e.getMessage());
		}
	}
}
