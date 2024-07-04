use staging;

CREATE TABLE tra_programs (
    id INT,
    name VARCHAR(255),
    date_from DATE,
    date_to DATE,
    is_store_active BOOLEAN,
    coin_name VARCHAR(50),
    is_deleted BOOLEAN
);

CREATE TABLE tra_participants (
    id INT,
    username VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    city VARCHAR(255),
    email VARCHAR(255),
    mobile VARCHAR(50),
    is_deleted BOOLEAN
);

CREATE TABLE tra_groups (
    id INT,
    name VARCHAR(255),
    level INT,
    program_id INT,
    is_deleted BOOLEAN
);

CREATE TABLE tra_address (
    id INT,
    city VARCHAR(255),
    sector VARCHAR(255),
    main_street VARCHAR(255),
    secondary_street VARCHAR(255),
    is_deleted BOOLEAN
);


CREATE TABLE tra_suppliers (
    id INT,
    name VARCHAR(255),
    contact_name VARCHAR(255),
    email VARCHAR(255),
    mobile VARCHAR(50),
    is_deleted BOOLEAN
);

CREATE TABLE tra_positions (
    id INT,
    name VARCHAR(255),
    max_points_per_month INT,
    program_id INT,
    is_deleted BOOLEAN
);

CREATE TABLE tra_awards (
    id INT,
    name VARCHAR(255),
    model VARCHAR(255),
    current_cost DECIMAL(10, 2),
    supplier_code VARCHAR(50),
    is_deleted BOOLEAN
);

CREATE TABLE tra_time (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    quarter INT,
    week INT
);

select * from tra_fact_requests
select * from tra_time where id = 4587
CREATE TABLE tra_fact_requests (
    fact_request_id INT AUTO_INCREMENT PRIMARY KEY,
    program_id INT,
    group_id INT,
    address_id INT,
    date_id INT,
    quantity INT,
    used_cost DECIMAL(10, 2),
    points INT
);

CREATE TABLE tra_fact_awards_claims (
    fact_awards_claim_id INT AUTO_INCREMENT PRIMARY KEY,
    participant_id INT,
    position_id INT,
    group_id INT,
    award_id INT,
    date_id INT,
    number_of_awards INT
);

CREATE TABLE tra_fact_supplier_awards (
    fact_supplier_awards_id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_id INT,
    award_id INT,
    program_id INT,
    date_id INT,
    total_cost DECIMAL(10, 2),
    awards_quantity INT
);

CREATE TABLE tra_fact_invoice_billing (
    fact_invoice_id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_id INT,
    program_id INT,
    date_id INT,
    total_billed DECIMAL(10, 2),
    discounts DECIMAL(10, 2),
    taxes DECIMAL(10, 2)
);


CREATE TABLE tra_invoice_items (
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