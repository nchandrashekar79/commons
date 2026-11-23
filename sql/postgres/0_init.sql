 --Section 1. Querying Data
--Select – show you how to query data from a single table.
--Column aliases – learn how to assign temporary names to columns or expressions within a query.
--Order By – guide you on how to sort the result set returned by a query.
--Select Distinct  – show you how to remove duplicate rows from the result set.

CREATE SCHEMA test_schema;

-- Create the employees table
CREATE TABLE employees (
    -- Auto-incrementing unique identifier (modern PostgreSQL standard)
    employee_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    
    -- Variable-length text fields with required constraints
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    
    -- Unique email constraint
    email VARCHAR(100) UNIQUE NOT NULL,
    
    -- Exact numeric type for currency (10 total digits, 2 after decimal)
    salary NUMERIC(10, 2) CHECK (salary >= 0),
    
    -- Date with a default value of the current date
    hire_date DATE DEFAULT CURRENT_DATE,
    
    -- Boolean flag defaulting to true
    is_active BOOLEAN DEFAULT TRUE
);

-- 2. Create the departments table
CREATE TABLE departments (
    department_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    department_name VARCHAR(50) UNIQUE NOT NULL,
    location VARCHAR(50) NOT NULL
);

-- Insert a sample row into the new table
INSERT INTO employees (first_name, last_name, email, salary)
VALUES ('Priya', 'Sharma', 'priya.sharma@example.com', 85000.00);

-- Insert 20 sample records into the employees table
INSERT INTO employees (first_name, last_name, email, salary, hire_date, is_active)
VALUES 
    ('Aarav', 'Patel', 'aarav.patel@example.com', 72000.00, '2023-01-15', TRUE),
    ('Ananya', 'Reddy', 'ananya.reddy@example.com', 88500.00, '2022-06-10', TRUE),
    ('Vikram', 'Rao', 'vikram.rao@example.com', 95000.00, '2021-11-03', TRUE),
    ('Meera', 'Nair', 'meera.nair@example.com', 64000.00, '2024-02-20', TRUE),
    ('Rohan', 'Verma', 'rohan.verma@example.com', 78000.00, '2023-05-12', FALSE),
    ('Kavya', 'Iyer', 'kavya.iyer@example.com', 105000.00, '2020-08-19', TRUE),
    ('Aditya', 'Kulkarni', 'aditya.kulkarni@example.com', 81000.00, '2022-09-25', TRUE),
    ('Sneha', 'Menon', 'sneha.menon@example.com', 69500.00, '2024-04-01', TRUE),
    ('Karthik', 'Subramanian', 'karthik.s@example.com', 112000.00, '2019-03-14', TRUE),
    ('Divya', 'Krishnan', 'divya.krishnan@example.com', 74500.00, '2023-07-30', TRUE),
    ('Siddharth', 'Joshi', 'siddharth.joshi@example.com', 91000.00, '2021-12-05', FALSE),
    ('Pooja', 'Desai', 'pooja.desai@example.com', 67000.00, '2024-06-18', TRUE),
    ('Nikhil', 'Choudhury', 'nikhil.c@example.com', 83000.00, '2022-04-11', TRUE),
    ('Nandini', 'Gowda', 'nandini.gowda@example.com', 98000.00, '2021-02-28', TRUE),
    ('Arjun', 'Malhotra', 'arjun.malhotra@example.com', 76000.00, '2023-09-09', TRUE),
    ('Shruti', 'Bhat', 'shruti.bhat@example.com', 62500.00, '2025-01-10', TRUE),
    ('Varun', 'Sinha', 'varun.sinha@example.com', 89000.00, '2022-10-17', TRUE),
    ('Lakshmi', 'Narayanan', 'lakshmi.n@example.com', 118000.00, '2018-07-22', TRUE),
    ('Pranav', 'Tiwari', 'pranav.tiwari@example.com', 71000.00, '2023-11-14', FALSE),
    ('Rhea', 'Kapoor', 'rhea.kapoor@example.com', 84500.00, '2022-08-03', TRUE);

-- 2. Insert 5 sample departments
INSERT INTO departments (department_name, location)
VALUES 
    ('Engineering', 'Hyderabad'),
    ('Product Management', 'Bengaluru'),
    ('Human Resources', 'Mumbai'),
    ('Finance', 'Delhi'),
    ('Research & Development', 'Pune');


-- 3. Add department_id foreign key column to employees
ALTER TABLE employees
ADD COLUMN department_id INT REFERENCES departments(department_id);

-- 4. Assign departments to your existing employees (leaving a few as NULL to test LEFT JOINs)
UPDATE employees SET department_id = 1 WHERE employee_id IN (1, 3, 6, 9, 14, 18); -- Engineering
UPDATE employees SET department_id = 2 WHERE employee_id IN (2, 7, 11, 17, 20);   -- Product Management
UPDATE employees SET department_id = 3 WHERE employee_id IN (4, 8, 12, 16);       -- Human Resources
UPDATE employees SET department_id = 4 WHERE employee_id IN (5, 10, 15);          -- Finance
-- Employees 13, 19, and 21 (Priya Sharma) remain NULL (unassigned)


select * from employees;



