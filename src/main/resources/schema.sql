# session聚合统计结果
CREATE TABLE `session_aggr_stat`
(
  # 任务标识，用户提交作业任务的ID
  `task_id`       INT(11) NOT NULL AUTO_INCREMENT,
  # 处理的session总量
  `session_count` INT(11) DEFAULT NULL,
  # session访问时长占比
  `1s_3s`         DOUBLE  DEFAULT NULL,
  `4s_6s`         DOUBLE  DEFAULT NULL,
  `7s_9s`         DOUBLE  DEFAULT NULL,
  `10s_30s`       DOUBLE  DEFAULT NULL,
  `30s_60s`       DOUBLE  DEFAULT NULL,
  `1m_3m`         DOUBLE  DEFAULT NULL,
  `3m_10m`        DOUBLE  DEFAULT NULL,
  `10m_30m`       DOUBLE  DEFAULT NULL,
  `30m`           DOUBLE  DEFAULT NULL,
  # session访问步长占比
  `1_3`           DOUBLE  DEFAULT NULL,
  `4_6`           DOUBLE  DEFAULT NULL,
  `7_9`           DOUBLE  DEFAULT NULL,
  `10_30`         DOUBLE  DEFAULT NULL,
  `30_60`         DOUBLE  DEFAULT NULL,
  `60`            DOUBLE  DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

-- 存储按照时间比例随机抽取出来的1000个session
drop table `session_random_extract`;

CREATE TABLE `session_random_extract`
(
  `task_id`         INT(11) NOT NULL ,
  `session_id`      VARCHAR(255) DEFAULT NULL,
  `start_time`      VARCHAR(50)  DEFAULT NULL,
  `search_keywords` VARCHAR(255) DEFAULT NULL,
  `click_category_ids` VARCHAR(255)  DEFAULT NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = gbk;

-- 存储按照点击、下单和支付排序出来的top10品类数据
CREATE TABLE `top10_category`
(
  `task_id`     INT(11) NOT NULL ,
  `category_id` INT(11) DEFAULT NULL,
  `click_count` INT(11) DEFAULT NULL,
  `order_count` INT(11) DEFAULT NULL,
  `pay_count`   INT(11) DEFAULT NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

-- 存储top10每个品类的点击top10的session
CREATE TABLE `top10_session`
(
  `task_id`     INT(11) NOT NULL,
  `category_id` INT(11)      DEFAULT NULL,
  `session_id`  VARCHAR(255) DEFAULT NULL,
  `click_count` INT(11)      DEFAULT NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

-- 用来存储随机抽取出来的session明细数据、top10品类的session明细数据
CREATE TABLE `session_detail`
(
  `task_id`            INT(11) NOT NULL ,
  `user_id`            INT(11)      DEFAULT NULL,
  `session_id`         VARCHAR(255) DEFAULT NULL,
  `page_id`            INT(11)      DEFAULT NULL,
  `action_time`        VARCHAR(255) DEFAULT NULL,
  `search_keyword`     VARCHAR(255) DEFAULT NULL,
  `click_category_id`  INT(11)      DEFAULT NULL,
  `click_product_id`   INT(11)      DEFAULT NULL,
  `order_category_ids` VARCHAR(255) DEFAULT NULL,
  `order_product_ids`  VARCHAR(255) DEFAULT NULL,
  `pay_category_ids`   VARCHAR(255) DEFAULT NULL,
  `pay_product_ids`    VARCHAR(255) DEFAULT NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

-- task表用来存储J2EE平台插入其中的任务信息
CREATE TABLE `task`
(
  `task_id`     INT(11) NOT NULL AUTO_INCREMENT,
  `task_name`   VARCHAR(255) DEFAULT NULL,
  `create_time` VARCHAR(255) DEFAULT NULL,
  `start_time`  VARCHAR(255) DEFAULT NULL,
  `finish_time` VARCHAR(255) DEFAULT NULL,
  `task_type`   VARCHAR(255) DEFAULT NULL,
  `task_status` VARCHAR(255) DEFAULT NULL,
  `task_param`  text,
  PRIMARY KEY (`task_id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 0
  DEFAULT CHARSET = utf8;
