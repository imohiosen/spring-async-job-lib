package com.imohiosen.asyncjob.application.executor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncExecutorPropertiesTest {

    @Test
    void defaultValues_areReasonable() {
        AsyncExecutorProperties props = AsyncExecutorProperties.DEFAULT;

        assertThat(props.corePoolSize()).isEqualTo(4);
        assertThat(props.maxPoolSize()).isEqualTo(16);
        assertThat(props.queueCapacity()).isEqualTo(100);
        assertThat(props.threadNamePrefix()).isEqualTo("async-job-");
    }

    @Test
    void customValues_arePreserved() {
        AsyncExecutorProperties props = new AsyncExecutorProperties(8, 32, 200, "my-worker-");

        assertThat(props.corePoolSize()).isEqualTo(8);
        assertThat(props.maxPoolSize()).isEqualTo(32);
        assertThat(props.queueCapacity()).isEqualTo(200);
        assertThat(props.threadNamePrefix()).isEqualTo("my-worker-");
    }

    @Test
    void equality_sameValues_areEqual() {
        AsyncExecutorProperties a = new AsyncExecutorProperties(4, 16, 100, "async-job-");
        AsyncExecutorProperties b = new AsyncExecutorProperties(4, 16, 100, "async-job-");

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void equality_differentValues_areNotEqual() {
        AsyncExecutorProperties a = new AsyncExecutorProperties(4, 16, 100, "async-job-");
        AsyncExecutorProperties b = new AsyncExecutorProperties(8, 16, 100, "async-job-");

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void default_equalsExplicitDefaults() {
        AsyncExecutorProperties explicit = new AsyncExecutorProperties(4, 16, 100, "async-job-");

        assertThat(AsyncExecutorProperties.DEFAULT).isEqualTo(explicit);
    }
}
