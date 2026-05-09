将检测文件，并执行导入到mysql中，利用版本号，避免全量导入的时候，删除文件了，访问不了的问题。

CREATE TABLE IF NOT EXISTS user_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    business_key VARCHAR(64) NOT NULL, -- 业务主键，如用户ID
    batch_version BIGINT NOT NULL,     -- 核心字段：版本号

    -- 【关键】必须建立联合索引，否则查询会全表扫描
    INDEX idx_version_key (batch_version, business_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

存储最新配置文件版本表
CREATE TABLE IF NOT EXISTS sys_config (
    namespace VARCHAR(64) PRIMARY KEY,
    config_key VARCHAR(64) PRIMARY KEY,
    config_value VARCHAR(255)
);