USE staging;

-- Tabla ext_programs
CREATE TABLE ext_programs AS SELECT * FROM oltp.programs WHERE 1=2;

-- Tabla ext_positions
CREATE TABLE ext_positions AS SELECT * FROM oltp.positions WHERE 1=2;

-- Tabla ext_groups
CREATE TABLE ext_groups AS SELECT * FROM oltp.`groups` WHERE 1=2;

-- Tabla ext_participants
CREATE TABLE ext_participants AS SELECT * FROM oltp.participants WHERE 1=2;

-- Tabla ext_address
CREATE TABLE ext_address AS SELECT * FROM oltp.address WHERE 1=2;

-- Tabla ext_suppliers
CREATE TABLE ext_suppliers AS SELECT * FROM oltp.suppliers WHERE 1=2;

-- Tabla ext_awards
CREATE TABLE ext_awards AS SELECT * FROM oltp.awards WHERE 1=2;

-- Tabla ext_invoices
CREATE TABLE ext_invoices AS SELECT * FROM oltp.invoices WHERE 1=2;

-- Tabla ext_invoices_items
CREATE TABLE ext_invoices_items AS SELECT * FROM oltp.invoices_items WHERE 1=2;

-- Tabla ext_requests
CREATE TABLE ext_requests AS SELECT * FROM oltp.requests WHERE 1=2;

-- Añadir más tablas según sea necesario

-- Confirmación
SELECT 'Tablas ext_ creadas correctamente en staging.';
