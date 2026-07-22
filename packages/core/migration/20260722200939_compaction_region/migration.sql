CREATE TABLE `compaction_region` (
	`session_id` text NOT NULL,
	`marker_id` text NOT NULL,
	`start_message_id` text NOT NULL,
	`summary_message_id` text NOT NULL,
	`summary_preview` text NOT NULL,
	`physical_message_count` integer NOT NULL,
	`semantic_message_count` integer NOT NULL,
	`part_count` integer NOT NULL,
	CONSTRAINT `compaction_region_pk` PRIMARY KEY(`session_id`, `marker_id`),
	CONSTRAINT `fk_compaction_region_session_id_session_id_fk` FOREIGN KEY (`session_id`) REFERENCES `session`(`id`) ON DELETE CASCADE
);
--> statement-breakpoint
CREATE INDEX `compaction_region_session_marker_idx` ON `compaction_region` (`session_id`,`marker_id`);--> statement-breakpoint
CREATE INDEX `compaction_region_session_start_idx` ON `compaction_region` (`session_id`,`start_message_id`);