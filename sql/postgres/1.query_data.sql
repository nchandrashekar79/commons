 --Section 1. Querying Data
--Select – show you how to query data from a single table.
--Column aliases – learn how to assign temporary names to columns or expressions within a query.
--Order By – guide you on how to sort the result set returned by a query.
--Select Distinct  – show you how to remove duplicate rows from the result set.

select * from employees;

-- Rename columns for a cleaner report
SELECT 
    employee_id AS id,
    first_name AS fname,
    last_name AS lname
FROM employees;


-- Sort employees from highest salary to lowest
SELECT 
    first_name, 
    last_name, 
    salary
FROM employees
ORDER BY salary DESC;

-- Sort employees chronologically by hire date (oldest hires first)
SELECT 
    first_name, 
    hire_date
FROM employees
ORDER BY hire_date ASC;

-- Find all unique active statuses in the table (returns only TRUE and FALSE)
SELECT DISTINCT is_active
FROM employees;


