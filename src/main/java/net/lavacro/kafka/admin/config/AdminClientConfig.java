package net.lavacro.kafka.admin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
}
