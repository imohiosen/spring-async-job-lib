package com.imohiosen.asyncjob.domain;

public enum JobStatus {
    SCHEDULED,
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    DEAD_LETTER
}
