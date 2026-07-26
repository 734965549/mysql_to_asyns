-- 初始化源数据库 - 创建数据库并设置字符集
CREATE DATABASE IF NOT EXISTS source_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci; -- 创建源数据库，使用UTF8MB4字符集和Unicode排序规则

-- 创建测试表
USE source_db; -- 切换到源数据库

-- 创建用户表
CREATE TABLE IF NOT EXISTS users ( -- 创建用户表
    id INT AUTO_INCREMENT PRIMARY KEY, -- 用户ID，自增主键
    username VARCHAR(50) NOT NULL, -- 用户名，最大50字符，不能为空
    email VARCHAR(100), -- 邮箱地址，最大100字符
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 创建时间，默认当前时间戳
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- 更新时间，默认当前时间戳，更新时自动更新
);

-- 创建订单表
CREATE TABLE IF NOT EXISTS orders ( -- 创建订单表
    id INT AUTO_INCREMENT PRIMARY KEY, -- 订单ID，自增主键
    user_id INT NOT NULL, -- 关联的用户ID，不能为空
    amount DECIMAL(10, 2), -- 订单金额，最大10位数字，其中2位小数
    status VARCHAR(20), -- 订单状态，最大20字符
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- 创建时间，默认当前时间戳
);

-- 插入测试数据 - 插入用户记录
INSERT INTO users (username, email) VALUES  -- 向用户表插入测试数据
('user1', 'user1@example.com'), -- 第1个用户
('user2', 'user2@example.com'), -- 第2个用户
('user3', 'user3@example.com'); -- 第3个用户

-- 插入测试数据 - 插入订单记录
INSERT INTO orders (user_id, amount, status) VALUES  -- 向订单表插入测试数据
(1, 100.00, 'completed'), -- 第1个订单，属于用户1，金额100，状态已完成
(2, 200.00, 'pending'), -- 第2个订单，属于用户2，金额200，状态待处理
(3, 150.00, 'completed'); -- 第3个订单，属于用户3，金额150，状态已完成

-- 授予同步用户权限 - 授予复制权限，用于增量同步
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'sync_user'@'%'; -- 授予sync_user从所有数据库复制和复制客户端权限
-- ALL 模式全量开始前短暂 FLUSH TABLES WITH READ LOCK 捕获 binlog 位点，需要 RELOAD（或 FLUSH_TABLES）
GRANT RELOAD ON *.* TO 'sync_user'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON source_db.* TO 'sync_user'@'%'; -- 授予sync_user对source_db数据库的增删改查权限
FLUSH PRIVILEGES; -- 刷新权限，使权限生效
