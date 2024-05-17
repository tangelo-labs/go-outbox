CREATE TABLE `outbox`
(
    `id` VARCHAR(191) NOT NULL,
    `event_name` VARCHAR(191) NOT NULL,
    `payload` BLOB NOT NULL,
    `metadata` JSON NOT NULL,
    `created_at` TIMESTAMP NOT NULL,
    `dispatched_at` TIMESTAMP NULL,
    `attempts` INT NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    KEY `idx_outbox_event_name` (`event_name`),
    KEY `idx_outbox_created_at` (`created_at`),
    KEY `idx_outbox_dispatched_at` (`dispatched_at`),
    KEY `idx_outbox_attempts` (`attempts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
