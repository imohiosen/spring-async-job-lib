package com.imohiosen.asyncjob.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskResultTest {

    @Test
    void success_isSuccess_returnsTrue() {
        TaskResult result = TaskResult.success("{\"ok\":true}");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.payload()).isEqualTo("{\"ok\":true}");
        assertThat(result.error()).isNull();
    }

    @Test
    void failure_isSuccess_returnsFalse() {
        RuntimeException ex = new RuntimeException("something went wrong");
        TaskResult result = TaskResult.failure(ex);
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.error()).isSameAs(ex);
        assertThat(result.payload()).isNull();
    }

    @Test
    void success_patternMatch_worksCorrectly() {
        TaskResult result = TaskResult.success("data");
        String output = switch (result) {
            case TaskResult.Success s -> "success:" + s.payload();
            case TaskResult.Failure f -> "failure";
        };
        assertThat(output).isEqualTo("success:data");
    }

    @Test
    void failure_patternMatch_worksCorrectly() {
        TaskResult result = TaskResult.failure(new IllegalStateException("oops"));
        String output = switch (result) {
            case TaskResult.Success s -> "success";
            case TaskResult.Failure f -> "failure:" + f.error().getMessage();
        };
        assertThat(output).isEqualTo("failure:oops");
    }
}
