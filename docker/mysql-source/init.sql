-- 初始化源数据库
CREATE DATABASE IF NOT EXISTS source_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 创建测试表
USE source_db;

CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入测试数据
INSERT INTO users (username, email) VALUES 
('user1', 'user1@example.com'),
('user2', 'user2@example.com'),
('user3', 'user3@example.com');

INSERT INTO orders (user_id, amount, status) VALUES 
(1, 100.00, 'completed'),
(2, 200.00, 'pending'),
(3, 150.00, 'completed');

-- 授予同步用户权限
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'sync_user'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON source_db.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;
