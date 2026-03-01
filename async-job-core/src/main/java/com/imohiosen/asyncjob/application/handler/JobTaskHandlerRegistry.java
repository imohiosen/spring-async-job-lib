package com.imohiosen.asyncjob.application.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Registry that maps {@link JobTaskHandler#taskType()} → handler instance.
 *
 * <p>Constructed with all available handler beans (typically injected by Spring).
 * Duplicate task types are rejected at construction time so misconfigurations
 * surface early.
 *
 * <h2>Spring wiring</h2>
 * <pre>{@code
 * @Bean
 * public JobTaskHandlerRegistry jobTaskHandlerRegistry(List<JobTaskHandler> handlers) {
 *     return new JobTaskHandlerRegistry(handlers);
 * }
 * }</pre>
 */
public class JobTaskHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(JobTaskHandlerRegistry.class);

    private final Map<String, JobTaskHandler> handlers;

    /**
     * @param handlers all available handler beans — must not contain duplicate task types
     * @throws IllegalStateException if two or more handlers declare the same task type
     */
    public JobTaskHandlerRegistry(List<JobTaskHandler> handlers) {
        if (handlers == null || handlers.isEmpty()) {
            log.warn("No JobTaskHandler beans found — DispatchingJobTaskConsumer will reject all messages");
            this.handlers = Collections.emptyMap();
            return;
        }

        Map<String, Long> counts = handlers.stream()
                .collect(Collectors.groupingBy(JobTaskHandler::taskType, Collectors.counting()));

        List<String> duplicates = counts.entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .map(Map.Entry::getKey)
                .toList();

        if (!duplicates.isEmpty()) {
            throw new IllegalStateException(
                    "Duplicate JobTaskHandler registrations for task types: " + duplicates);
        }

        this.handlers = handlers.stream()
                .collect(Collectors.toMap(JobTaskHandler::taskType, Function.identity()));

        log.info("Registered {} job task handler(s): {}", this.handlers.size(), this.handlers.keySet());
    }

    /**
     * Look up a handler by task type.
     *
     * @param taskType the task type string (from {@link com.imohiosen.asyncjob.domain.JobTask#taskType()})
     * @return the handler, or empty if none is registered for this type
     */
    public Optional<JobTaskHandler> getHandler(String taskType) {
        return Optional.ofNullable(handlers.get(taskType));
    }

    /**
     * @return {@code true} if a handler is registered for the given task type
     */
    public boolean hasHandler(String taskType) {
        return handlers.containsKey(taskType);
    }

    /**
     * @return an unmodifiable view of all registered task types
     */
    public Set<String> registeredTypes() {
        return Collections.unmodifiableSet(handlers.keySet());
    }
}
