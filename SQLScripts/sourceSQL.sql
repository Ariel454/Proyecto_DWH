USE oltp

CREATE TABLE programs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    date_from DATE,
    date_to DATE,
    logo VARCHAR(255),
    is_store_active BOOLEAN,
    main_banner VARCHAR(255),
    faq TEXT,
    rules TEXT,
    how_to_earn_points TEXT,
    support_phone VARCHAR(50),
    terms_and_conditions TEXT,
    coin_name VARCHAR(50),
    is_demo BOOLEAN,
    has_academy BOOLEAN,
    academy_url VARCHAR(255),
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    initial_points_bag INT
);


CREATE TABLE positions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    max_points_per_month INT,
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    program_id INT,
    crp BOOLEAN,
    FOREIGN KEY (program_id) REFERENCES programs(id)
);


CREATE TABLE `groups` (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    level INT,
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    program_id INT,
    registered_by_participant_id INT,
    parent_id INT,
    supervisor_id INT,
    code VARCHAR(50),
    mobile VARCHAR(50),
    can_upload_snaps BOOLEAN,
    can_upload_extra_snaps BOOLEAN
);

CREATE TABLE participants (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(255),
    identifier VARCHAR(255),
    password VARCHAR(255),
    avatar TEXT,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    mobile VARCHAR(50),
    email VARCHAR(255),
    is_active BOOLEAN,
    activated_at TIMESTAMP NULL DEFAULT NULL,
    is_a_consumer_owner BOOLEAN,
    is_a_consumer_registrar BOOLEAN,
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    program_id INT,
    position_id INT,
    group_id INT,
    participant_supervisor_id INT,
    document VARCHAR(255),
    date_of_birth DATE,
    is_approved BOOLEAN,
    approved_at TIMESTAMP NULL DEFAULT NULL,
    city VARCHAR(255),
    created_from VARCHAR(255),
    is_vip BOOLEAN,
    is_for_test BOOLEAN,
    approved_policy_at TIMESTAMP NULL DEFAULT NULL,
    approved_terms_and_conditions_at TIMESTAMP NULL DEFAULT NULL,
    approved_terms_and_conditions VARCHAR(255),
    segment_id INT,
    can_make_requests BOOLEAN,
    cannot_make_requests_reason TEXT,
    is_v5 BOOLEAN,
    FOREIGN KEY (program_id) REFERENCES programs(id),
    FOREIGN KEY (position_id) REFERENCES positions(id),
    FOREIGN KEY (group_id) REFERENCES `groups`(id),
    FOREIGN KEY (participant_supervisor_id) REFERENCES participants(id)
);

ALTER TABLE `groups`
ADD CONSTRAINT fk_groups_program_id
FOREIGN KEY (program_id) REFERENCES programs(id),
ADD CONSTRAINT fk_groups_registered_by_participant_id
FOREIGN KEY (registered_by_participant_id) REFERENCES participants(id),
ADD CONSTRAINT fk_groups_supervisor_id
FOREIGN KEY (supervisor_id) REFERENCES participants(id);


CREATE TABLE address (
    id INT PRIMARY KEY AUTO_INCREMENT,
    alias VARCHAR(255),
    sector VARCHAR(255),
    main_street VARCHAR(255),
    house_number VARCHAR(50),
    secondary_street VARCHAR(255),
    reference TEXT,
    contact_name VARCHAR(255),
    contact_phone VARCHAR(50),
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    participant_id INT,
    city VARCHAR(255),
    FOREIGN KEY (participant_id) REFERENCES participants(id)
);

CREATE TABLE suppliers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(255),
    name VARCHAR(255),
    mobile VARCHAR(50),
    contact_name VARCHAR(255),
    email VARCHAR(255),
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    margin DECIMAL(10, 2)
);

CREATE TABLE awards (
    id INT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(255),
    name VARCHAR(255),
    model VARCHAR(255),
    description TEXT,
    main_image VARCHAR(255),
    current_cost DECIMAL(10, 2),
    last_cost_updated_date TIMESTAMP NULL DEFAULT NULL,
    is_active BOOLEAN,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    brand_id INT,
    supplier_code VARCHAR(50),
    FOREIGN KEY (brand_id) REFERENCES suppliers(id)
);

CREATE TABLE invoices (
    id INT PRIMARY KEY AUTO_INCREMENT,
    attachment TEXT,
    has_individual_values BOOLEAN,
    invoice_number VARCHAR(255),
    discount DECIMAL(10, 2),
    delivery DECIMAL(10, 2),
    taxes DECIMAL(10, 2),
    is_deleted BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    supplier_id INT,
    subtotal DECIMAL(10, 2),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
);

CREATE TABLE invoices_items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    quantity INT,
    name VARCHAR(255),
    supplier_code VARCHAR(50),
    cost DECIMAL(10, 2),
    delivery DECIMAL(10, 2),
    is_gift BOOLEAN,
    has_special_cost BOOLEAN,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    invoice_id INT,
    total DECIMAL(10, 2),
    type VARCHAR(50),
    reference_unit_cost DECIMAL(10, 2),
    comment TEXT,
    FOREIGN KEY (invoice_id) REFERENCES invoices(id)
);


CREATE TABLE requests (
    id INT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(255),
    quantity INT,
    used_cost DECIMAL(10, 2),
    margin DECIMAL(10, 2),
    points INT,
    approved_at TIMESTAMP,
    downloaded_at TIMESTAMP,
    newer_at TIMESTAMP,
    delivered_at TIMESTAMP,
    warehouse_at TIMESTAMP,
    dispatched_at TIMESTAMP,
    canceled_at TIMESTAMP,
    cancelation_reason TEXT,
    shipping_guide TEXT,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    award_id INT,
    address_id INT,
    type VARCHAR(50),
    status VARCHAR(50),
    invoice_id INT,
    courier VARCHAR(255),
    special_at TIMESTAMP,
    invoice_number VARCHAR(255),
    invoice_cost DECIMAL(10, 2),
    billing_status VARCHAR(50),
    invoice_discount DECIMAL(10, 2),
    invoice_interest DECIMAL(10, 2),
    invoice_delivery DECIMAL(10, 2),
    is_deleted BOOLEAN,
    FOREIGN KEY (award_id) REFERENCES awards(id),
    FOREIGN KEY (address_id) REFERENCES address(id),
    FOREIGN KEY (invoice_id) REFERENCES invoices(id)
);