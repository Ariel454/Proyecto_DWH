use sor
CREATE TABLE Dim_Programs (
    program_id INT PRIMARY KEY,
    name VARCHAR(255),
    date_from DATE,
    date_to DATE,
    is_store_active BOOLEAN,
    coin_name VARCHAR(50)
);

CREATE TABLE Dim_Participants (
    participant_id INT PRIMARY KEY,
    username VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    city VARCHAR(255),
    email VARCHAR(255),
    mobile VARCHAR(50)
);


CREATE TABLE Dim_Groups (
    group_id INT PRIMARY KEY,
    group_name VARCHAR(255),
    level INT,
    program_id INT,
    FOREIGN KEY (program_id) REFERENCES Dim_Programs(program_id)
);

CREATE TABLE Dim_Address (
    address_id INT PRIMARY KEY,
    city VARCHAR(255),
    sector VARCHAR(255),
    main_street VARCHAR(255),
    secondary_street VARCHAR(255)
);

CREATE TABLE Dim_Suppliers (
    supplier_id INT PRIMARY KEY,
    name VARCHAR(255),
    contact_name VARCHAR(255),
    email VARCHAR(255),
    mobile VARCHAR(50)
);

CREATE TABLE Dim_Positions (
    position_id INT PRIMARY KEY,
    name VARCHAR(255),
    max_points_per_month INT,
    program_id INT,
    FOREIGN KEY (program_id) REFERENCES Dim_Programs(program_id)
);


CREATE TABLE Dim_Awards (
    award_id INT PRIMARY KEY,
    name VARCHAR(255),
    model VARCHAR(255),
    current_cost DECIMAL(10, 2),
    supplier_code VARCHAR(50)
);


CREATE TABLE Dim_Time (
    date_id INT PRIMARY KEY AUTO_INCREMENT,
    date DATE,
    day INT,
    month INT,
    year INT,
    quarter INT,
    week INT
);

USE sor;
SHOW TABLES;
SELECT COUNT(*) FROM Dim_Groups;
SELECT * FROM Dim_Groups LIMIT 10;


CREATE TABLE Fact_Requests (
     fact_request_id INT PRIMARY KEY AUTO_INCREMENT,
     program_id INT,
     group_id INT,
     address_id INT,
     date_id INT,
     quantity INT,
     used_cost DECIMAL(10, 2),
     points INT,
     FOREIGN KEY (program_id) REFERENCES Dim_Programs(program_id),
     FOREIGN KEY (group_id) REFERENCES Dim_Groups(group_id),
     FOREIGN KEY (address_id) REFERENCES Dim_Address(address_id),
     FOREIGN KEY (date_id) REFERENCES Dim_Time(date_id)
 );


CREATE TABLE Fact_Awards_Claims (
    fact_awards_claim_id INT PRIMARY KEY AUTO_INCREMENT,
    participant_id INT,
    position_id INT,
    group_id INT,
    award_id INT,
    date_id INT,
    number_of_awards INT,
    FOREIGN KEY (participant_id) REFERENCES Dim_Participants(participant_id),
    FOREIGN KEY (position_id) REFERENCES Dim_Positions(position_id),
    FOREIGN KEY (group_id) REFERENCES Dim_Groups(group_id),
    FOREIGN KEY (award_id) REFERENCES Dim_Awards(award_id),
    FOREIGN KEY (date_id) REFERENCES Dim_Time(date_id)
);

CREATE TABLE Fact_Supplier_Awards (
    fact_supplier_awards_id INT PRIMARY KEY AUTO_INCREMENT,
    supplier_id INT,
    award_id INT,
    program_id INT,
    date_id INT,
    total_cost DECIMAL(10, 2),
    awards_quantity INT,
    FOREIGN KEY (supplier_id) REFERENCES Dim_Suppliers(supplier_id),
    FOREIGN KEY (award_id) REFERENCES Dim_Awards(award_id),
    FOREIGN KEY (program_id) REFERENCES Dim_Programs(program_id),
    FOREIGN KEY (date_id) REFERENCES Dim_Time(date_id)
);

CREATE TABLE Fact_Invoice_Billing (
    fact_invoice_id INT PRIMARY KEY AUTO_INCREMENT,
    supplier_id INT,
    program_id INT,
    date_id INT,
    total_billed DECIMAL(10, 2),
    discounts DECIMAL(10, 2),
    taxes DECIMAL(10, 2),
    FOREIGN KEY (supplier_id) REFERENCES Dim_Suppliers(supplier_id),
    FOREIGN KEY (program_id) REFERENCES Dim_Programs(program_id),
    FOREIGN KEY (date_id) REFERENCES Dim_Time(date_id)
);

CREATE TABLE Fact_Invoice_Items (
    fact_invoice_item_id INT PRIMARY KEY AUTO_INCREMENT,
    invoice_id INT,
    item_name VARCHAR(255),
    quantity INT,
    cost DECIMAL(10, 2),
    total DECIMAL(10, 2),
    date_id INT,
    FOREIGN KEY (invoice_id) REFERENCES Fact_Invoice_Billing(fact_invoice_id),
    FOREIGN KEY (date_id) REFERENCES Dim_Time(date_id)
);

select * from Dim_Programs
