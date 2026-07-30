package net.lavacro.kafka.admin.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.lavacro.kafka.admin.services.TopicsService;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaMetricsCollectorTests {
	private static final TopicPartition PARTITION = new TopicPartition("orders", 0);

	private static class StubTopicsService extends TopicsService {
		StubTopicsService() {
			super(null);
		}

		@Override
		public Map<String, Integer> partitionCounts() {
			return Map.of("orders", 3, "payments", 1);
		}

		@Override
		public List<String> consumerGroupIds() {
			return List.of("billing");
		}

		@Override
		public Map<TopicPartition, Long> committedOffsets(String groupId) {
			return Map.of(PARTITION, 10L);
		}

		@Override
		public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
			return Map.of(PARTITION, 25L);
		}
	}

	@Test
	void refreshRegistersTopicAndConsumerGroupGauges() {
		MeterRegistry registry = new SimpleMeterRegistry();
		new KafkaMetricsCollector(new StubTopicsService(), registry).refresh();

		assertThat(registry.get("kafka_topic_count").gauge().value()).isEqualTo(2.0);
		assertThat(registry.get("kafka_topic_partitions").tag("topic", "orders").gauge().value()).isEqualTo(3.0);
		assertThat(registry.get("kafka_consumergroup_current_offset")
			.tags("group", "billing", "topic", "orders", "partition", "0").gauge().value()).isEqualTo(10.0);
		assertThat(registry.get("kafka_consumergroup_end_offset")
			.tags("group", "billing", "topic", "orders", "partition", "0").gauge().value()).isEqualTo(25.0);
		assertThat(registry.get("kafka_consumergroup_lag")
			.tags("group", "billing", "topic", "orders", "partition", "0").gauge().value()).isEqualTo(15.0);
	}
}
