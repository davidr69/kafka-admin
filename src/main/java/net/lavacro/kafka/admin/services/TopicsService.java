package net.lavacro.kafka.admin.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

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

	/**
	 * Sorted list of topic names in the cluster, empty if the cluster cannot be reached.
	 */
	public List<String> topicNames() {
		try {
			List<String> topics = new ArrayList<>(adminClient.listTopics().names().get());
			Collections.sort(topics);
			return topics;
		} catch(Exception e) {
			log.error("Unable to list topics: {}", e.getMessage());
			return List.of();
		}
	}

	/**
	 * Number of partitions per topic, keyed by topic name.
	 */
	public Map<String, Integer> partitionCounts() {
		List<String> topics = topicNames();
		if (topics.isEmpty()) {
			return Map.of();
		}
		Map<String, Integer> counts = new TreeMap<>();
		DescribeTopicsResult result = adminClient.describeTopics(topics);
		result.topicNameValues().forEach((name, future) -> {
			try {
				TopicDescription description = future.get();
				counts.put(name, description.partitions().size());
			} catch(Exception e) {
				log.error("Unable to describe topic {}: {}", name, e.getMessage());
			}
		});
		return counts;
	}

	/**
	 * Ids of the consumer groups known to the cluster.
	 */
	@SuppressWarnings("deprecation")
	public List<String> consumerGroupIds() {
		try {
			return adminClient.listConsumerGroups().valid().get().stream()
				.map(org.apache.kafka.clients.admin.ConsumerGroupListing::groupId)
				.sorted()
				.toList();
		} catch(Exception e) {
			log.error("Unable to list consumer groups: {}", e.getMessage());
			return List.of();
		}
	}

	/**
	 * Committed offsets of a consumer group, keyed by topic partition.
	 */
	public Map<TopicPartition, Long> committedOffsets(String groupId) {
		try {
			Map<TopicPartition, OffsetAndMetadata> offsets =
				adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
			Map<TopicPartition, Long> committed = new LinkedHashMap<>();
			offsets.forEach((partition, offset) -> {
				if (offset != null) {
					committed.put(partition, offset.offset());
				}
			});
			return committed;
		} catch(Exception e) {
			log.error("Unable to list offsets for consumer group {}: {}", groupId, e.getMessage());
			return Map.of();
		}
	}

	/**
	 * Log end offsets for the given topic partitions.
	 */
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
		if (partitions.isEmpty()) {
			return Map.of();
		}
		try {
			Map<TopicPartition, OffsetSpec> request = partitions.stream()
				.collect(Collectors.toMap(Function.identity(), partition -> OffsetSpec.latest()));
			Map<TopicPartition, Long> endOffsets = new LinkedHashMap<>();
			adminClient.listOffsets(request).all().get()
				.forEach((partition, info) -> endOffsets.put(partition, info.offset()));
			return endOffsets;
		} catch(Exception e) {
			log.error("Unable to list end offsets: {}", e.getMessage());
			return Map.of();
		}
	}
}
