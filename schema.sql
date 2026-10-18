-- Adaptive Bank OS Database Schema
-- Compatible with MySQL 8.0+

CREATE DATABASE IF NOT EXISTS bank_os;
USE bank_os;

-- 1. Accounts Table
CREATE TABLE IF NOT EXISTS Accounts (
    AccountID INT PRIMARY KEY,
    AccountType VARCHAR(32) NOT NULL,
    Balance DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_positive_account_id CHECK (AccountID > 0)
) ENGINE=InnoDB;

-- 2. Transactions Table
CREATE TABLE IF NOT EXISTS Transactions (
    TransactionID INT AUTO_INCREMENT PRIMARY KEY,
    PID INT NOT NULL,
    Type VARCHAR(32) NOT NULL,
    FromAccount INT NOT NULL,
    ToAccount INT NOT NULL,
    Amount DECIMAL(15, 2) NOT NULL,
    Status VARCHAR(32) NOT NULL, -- 'TERMINATED', 'TERMINATED_FAILED', etc.
    Reason VARCHAR(255) DEFAULT '',
    CreatedTick INT NOT NULL,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_pid (PID),
    INDEX idx_status (Status),
    FOREIGN KEY (FromAccount) REFERENCES Accounts(AccountID) ON DELETE RESTRICT,
    FOREIGN KEY (ToAccount) REFERENCES Accounts(AccountID) ON DELETE RESTRICT
) ENGINE=InnoDB;

-- 3. Write-Ahead Log (WAL) Table
CREATE TABLE IF NOT EXISTS WriteAheadLog (
    LogID INT AUTO_INCREMENT PRIMARY KEY,
    SequenceNumber BIGINT NOT NULL UNIQUE,
    PID INT NOT NULL,
    Operation VARCHAR(32) NOT NULL, -- 'PREPARE', 'DEBIT', 'CREDIT', 'COMMIT', 'ROLLBACK'
    AccountID INT NOT NULL,
    OldBalance DECIMAL(15, 2) NOT NULL,
    NewBalance DECIMAL(15, 2) NOT NULL,
    Status VARCHAR(32) NOT NULL,
    Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_wal_seq (SequenceNumber),
    INDEX idx_wal_pid (PID)
) ENGINE=InnoDB;

-- Seed Initial System Accounts
INSERT INTO Accounts (AccountID, AccountType, Balance) VALUES
(1001, 'RETAIL', 50000.00),
(1002, 'RETAIL', 50000.00),
(1003, 'RETAIL', 50000.00),
(1004, 'RETAIL', 50000.00),
(1005, 'RETAIL', 300.00),    -- Low balance for insufficient funds rollback testing
(1006, 'RETAIL', 50000.00),
(1007, 'RETAIL', 50000.00),
(1008, 'RETAIL', 100.00),    -- Low balance for insufficient funds rollback testing
(2001, 'CORPORATE', 5000000.00),
(2002, 'CORPORATE', 5000000.00),
(5001, 'ATM_VAULT', 1000000.00),
(5002, 'ATM_VAULT', 1000000.00)
ON DUPLICATE KEY UPDATE Balance = VALUES(Balance);
