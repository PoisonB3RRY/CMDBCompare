CREATE DATABASE IF NOT EXISTS cmdb_compare DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE cmdb_compare;

CREATE TABLE IF NOT EXISTS `t_filter_rule` (
  `rule_id` varchar(64) NOT NULL COMMENT '规则ID',
  `rule_name` varchar(128) NOT NULL COMMENT '规则名称',
  `rule_description` varchar(255) DEFAULT NULL COMMENT '规则描述',
  `rule_dimension` varchar(32) DEFAULT NULL COMMENT '规则维度',
  `rule_category` varchar(32) DEFAULT NULL COMMENT '分类',
  `rule_type` varchar(32) DEFAULT NULL COMMENT '类型',
  `rule_definition` json DEFAULT NULL COMMENT '规则定义的内容JSON',
  `enabled` tinyint(1) DEFAULT '1' COMMENT '是否开启',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_user` varchar(64) DEFAULT NULL COMMENT '创建者',
  `sort_order` int DEFAULT '0' COMMENT '排序',
  PRIMARY KEY (`rule_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='过滤规则表';

CREATE TABLE IF NOT EXISTS `t_reconciliation_config` (
  `config_id` varchar(64) NOT NULL COMMENT '配置ID',
  `task_name` varchar(128) NOT NULL COMMENT '任务模版名称',
  `source_file_id` varchar(64) DEFAULT NULL COMMENT '源文件ID',
  `rp_file_id` varchar(64) DEFAULT NULL COMMENT '目标文件ID',
  `primary_key` varchar(128) DEFAULT NULL COMMENT '对比主键串',
  `compare_fields` json DEFAULT NULL COMMENT '比对字段JSON数组',
  `field_mapping` json DEFAULT NULL COMMENT '字段映射关系',
  `enable_filter` tinyint(1) DEFAULT '1' COMMENT '是否启用过滤',
  `filter_rule_ids` json DEFAULT NULL COMMENT '规则ID集合JSON',
  `filter_logic` varchar(16) DEFAULT 'AND' COMMENT '规则关系AND/OR',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(64) DEFAULT NULL,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `status` varchar(32) DEFAULT 'READY' COMMENT '状态',
  PRIMARY KEY (`config_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账配置表';

CREATE TABLE IF NOT EXISTS `t_reconciliation_task` (
  `task_id` varchar(64) NOT NULL COMMENT '流水任务ID',
  `task_name` varchar(128) NOT NULL COMMENT '执行任务名称',
  `config_id` varchar(64) DEFAULT NULL COMMENT '关联配置ID',
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `duration` bigint DEFAULT NULL COMMENT '耗时ms',
  `status` varchar(32) DEFAULT 'RUNNING' COMMENT '状态',
  `error_msg` text COMMENT '报错信息',
  `source_record_count` bigint DEFAULT '0' COMMENT '源记录数',
  `rp_record_count_before` bigint DEFAULT '0' COMMENT '目标提取前记录数',
  `rp_record_count_after` bigint DEFAULT '0' COMMENT '目标提取后记录数',
  `diff1_count` bigint DEFAULT '0' COMMENT '源独有',
  `diff2_count` bigint DEFAULT '0' COMMENT '目标独有',
  `diff3_count` bigint DEFAULT '0' COMMENT '有差异',
  `diff4_count` bigint DEFAULT '0' COMMENT '完全一致',
  `result_path` varchar(255) DEFAULT NULL COMMENT '生成结果路径',
  `livy_batch_id` int DEFAULT NULL COMMENT 'Livy批处理ID',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账任务流水表';
