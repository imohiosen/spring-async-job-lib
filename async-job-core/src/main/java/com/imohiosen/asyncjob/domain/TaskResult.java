package com.imohiosen.asyncjob.domain;

/**
 * Sealed result type returned by {@code AbstractJobTaskConsumer.processTask()}.
 * Use {@link #success(String)} or {@link #failure(Throwable)} factory methods.
 */
public sealed interface TaskResult permits TaskResult.Success, TaskResult.Failure {

    record Success(String payload) implements TaskResult {}

    record Failure(Throwable error) implements TaskResult {}

    static TaskResult success(String payload) {
        return new Success(payload);
    }

    static TaskResult failure(Throwable error) {
        return new Failure(error);
    }

    default boolean isSuccess() {
        return this instanceof Success;
    }

    default String payload() {
        return switch (this) {
            case Success s -> s.payload();
            case Failure f -> null;
        };
    }

    default Throwable error() {
        return switch (this) {
            case Success s -> null;
            case Failure f -> f.error();
        };
    }
}
