package net.lavacro.kafka.admin.config;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class AdminClientConfig {
	@Value("${app.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public AdminClient adminClient() {
		return AdminClient.create(
			java.util.Collections.singletonMap("bootstrap.servers", bootstrapServers)
		);
	}

	@PreDestroy
	public void destroy() {
		adminClient().close(Duration.ofSeconds(10));
	}
}
