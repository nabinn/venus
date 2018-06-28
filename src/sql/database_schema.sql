CREATE DATABASE IF NOT EXISTS venmo_db;

USE venmo_db;

CREATE TABLE IF NOT EXISTS users(
                                id INT PRIMARY KEY,
                                username VARCHAR(50) NOT NULL,
                                firstname VARCHAR(100),
                                lastname VARCHAR(100),
                                picture TEXT
                                );
CREATE TABLE IF NOT EXISTS sender_activity(
                                user_id INT NOT NULL,
                                transaction_date DATE,
                                send_freq INT,
                                PRIMARY KEY(user_id, transaction_date),
                                FOREIGN KEY (user_id) REFERENCES users(id)
                                );
                                
CREATE TABLE IF NOT EXISTS receiver_activity(
                                user_id INT NOT NULL,
                                transaction_date DATE,
                                recv_freq INT,
                                PRIMARY KEY(user_id, transaction_date),
                                FOREIGN KEY (user_id) REFERENCES users(id)
                                );
                                
                                
CREATE TABLE IF NOT EXISTS pair_activity(
                                user1 INT NOT NULL,
                                user2 INT NOT NULL,
                                transaction_date DATE,
                                transaction_freq INT,
                                PRIMARY KEY (user1, user2, transaction_date),
                                FOREIGN KEY (user1) REFERENCES users(id),
                                FOREIGN KEY (user2) REFERENCES users(id)
                                );
                                
CREATE TABLE IF NOT EXISTS net_spending(
                                user_id INT NOT NULL,
                                transaction_date DATE,
                                amount DECIMAL,
                                PRIMARY KEY (user_id, transaction_date),
                                FOREIGN KEY (user_id) REFERENCES users(id)
                                );

