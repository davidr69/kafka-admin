package net.lavacro.kafka.admin.api.v1;

import lombok.RequiredArgsConstructor;
import net.lavacro.kafka.admin.services.TopicsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api/v1/topics")
@RequiredArgsConstructor
public class Topics {
	private final TopicsService topicsService;

	@GetMapping
	public void topics() {
		topicsService.topics();
	}
}
