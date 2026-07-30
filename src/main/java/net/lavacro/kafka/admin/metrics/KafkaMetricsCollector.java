package net.lavacro.kafka.admin.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import net.lavacro.kafka.admin.services.TopicsService;
import org.apache.kafka.common.TopicPartition;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class KafkaMetricsCollector {
	private final TopicsService topicsService;
	private final AtomicInteger topicCount = new AtomicInteger(0);
	private final MultiGauge topicPartitions;
	private final MultiGauge groupCommittedOffset;
	private final MultiGauge groupEndOffset;
	private final MultiGauge groupLag;

	public KafkaMetricsCollector(TopicsService topicsService, MeterRegistry registry) {
		this.topicsService = topicsService;
		registry.gauge("kafka_topic_count", topicCount);
		this.topicPartitions = MultiGauge.builder("kafka_topic_partitions")
			.description("Number of partitions per topic")
			.register(registry);
		this.groupCommittedOffset = MultiGauge.builder("kafka_consumergroup_current_offset")
			.description("Committed offset of a consumer group for a topic partition")
			.register(registry);
		this.groupEndOffset = MultiGauge.builder("kafka_consumergroup_end_offset")
			.description("Log end offset of a topic partition consumed by a consumer group")
			.register(registry);
		this.groupLag = MultiGauge.builder("kafka_consumergroup_lag")
			.description("Lag of a consumer group for a topic partition")
			.register(registry);
	}

	@PostConstruct
	public void init() {
		log.info("Kafka metrics collector initialised");
	}

	@Scheduled(fixedDelayString = "${app.kafka.metrics.interval:30000}")
	public void refresh() {
		try {
			refreshTopics();
			refreshConsumerGroups();
		} catch (Exception e) {
			log.error("Failed to refresh Kafka metrics: {}", e.getMessage(), e);
		}
	}

	private void refreshTopics() {
		Map<String, Integer> partitionCounts = topicsService.partitionCounts();
		topicCount.set(partitionCounts.size());
		List<MultiGauge.Row<?>> rows = new ArrayList<>();
		partitionCounts.forEach((topic, count) ->
			rows.add(MultiGauge.Row.of(Tags.of("topic", topic), count)));
		topicPartitions.register(rows, true);
	}

	private void refreshConsumerGroups() {
		List<MultiGauge.Row<?>> committedRows = new ArrayList<>();
		List<MultiGauge.Row<?>> endRows = new ArrayList<>();
		List<MultiGauge.Row<?>> lagRows = new ArrayList<>();

		for (String groupId : topicsService.consumerGroupIds()) {
			Map<TopicPartition, Long> committed = topicsService.committedOffsets(groupId);
			if (committed.isEmpty()) {
				continue;
			}
			Map<TopicPartition, Long> endOffsets = topicsService.endOffsets(committed.keySet());
			committed.forEach((partition, committedOffset) -> {
				Tags tags = Tags.of(
					"group", groupId,
					"topic", partition.topic(),
					"partition", String.valueOf(partition.partition()));
				committedRows.add(MultiGauge.Row.of(tags, committedOffset));
				Long endOffset = endOffsets.get(partition);
				if (endOffset != null) {
					endRows.add(MultiGauge.Row.of(tags, endOffset));
					lagRows.add(MultiGauge.Row.of(tags, Math.max(0L, endOffset - committedOffset)));
				}
			});
		}

		groupCommittedOffset.register(committedRows, true);
		groupEndOffset.register(endRows, true);
		groupLag.register(lagRows, true);
	}
}
