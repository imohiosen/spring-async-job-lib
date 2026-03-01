package com.imohiosen.asyncjob.application.executor;

/**
 * Configuration properties for the async job task executor thread pool.
 *
 * @param corePoolSize     core number of threads kept alive (default 4)
 * @param maxPoolSize      maximum thread count (default 16)
 * @param queueCapacity    work queue depth before rejection (default 100)
 * @param threadNamePrefix thread name prefix for thread dumps (default "async-job-")
 */
public record AsyncExecutorProperties(
        int    corePoolSize,
        int    maxPoolSize,
        int    queueCapacity,
        String threadNamePrefix
) {
    public static final AsyncExecutorProperties DEFAULT =
            new AsyncExecutorProperties(4, 16, 100, "async-job-");
}
