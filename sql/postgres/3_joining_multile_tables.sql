-- ============================================================================
-- PostgreSQL Joins Practice Queries
-- Designed to run against the `employees` and `departments` tables.
-- ============================================================================

/* ----------------------------------------------------------------------------
   1. Joins (Overview)
   Joins combine rows from two or more tables based on a related column 
   between them (here, `employees.department_id` and `departments.department_id`).
   ---------------------------------------------------------------------------- */
SELECT 
    employees.first_name,
    employees.last_name,
    departments.department_name
FROM employees
JOIN departments 
    ON employees.department_id = departments.department_id;


/* ----------------------------------------------------------------------------
   2. Table Aliases
   Table aliases assign temporary short names (like `e` and `d`) to tables 
   within a query to make SQL cleaner, shorter, and easier to read.
   ---------------------------------------------------------------------------- */
SELECT 
    e.employee_id,
    e.first_name,
    e.salary,
    d.department_name,
    d.location
FROM employees AS e
INNER JOIN departments AS d 
    ON e.department_id = d.department_id;


/* ----------------------------------------------------------------------------
   3. Inner Join
   Selects rows from `employees` that have matching rows in `departments`.
   (Excludes the 3 employees with NULL department_id and the empty R&D dept).
   ---------------------------------------------------------------------------- */
SELECT 
    e.first_name,
    e.last_name,
    d.department_name
FROM employees e
INNER JOIN departments d 
    ON e.department_id = d.department_id
ORDER BY d.department_name, e.first_name;


/* ----------------------------------------------------------------------------
   4. Left Join (Left Outer Join)
   Selects ALL rows from the left table (`employees`), along with matching 
   rows from the right table (`departments`). Unassigned employees show NULL.
   ---------------------------------------------------------------------------- */
SELECT 
    e.employee_id,
    e.first_name,
    e.last_name,
    d.department_name
FROM employees e
LEFT JOIN departments d 
    ON e.department_id = d.department_id
ORDER BY e.employee_id;


/* ----------------------------------------------------------------------------
   5. Self-Join
   Joins a table to itself using two different aliases (`e1` and `e2`).
   Example: Find pairs of employees who work in the exact same department.
   ---------------------------------------------------------------------------- */
SELECT 
    e1.first_name || ' ' || e1.last_name AS employee_1,
    e2.first_name || ' ' || e2.last_name AS employee_2,
    e1.department_id
FROM employees e1
INNER JOIN employees e2 
    ON e1.department_id = e2.department_id
   AND e1.employee_id < e2.employee_id -- Prevents self-pairing and duplicate pairs
ORDER BY e1.department_id, employee_1;


/* ----------------------------------------------------------------------------
   6. Full Outer Join
   Returns all rows when there is a match in EITHER table. Useful for finding 
   unmatched records on both sides (unassigned employees AND empty departments).
   ---------------------------------------------------------------------------- */
SELECT 
    e.first_name,
    e.last_name,
    d.department_name
FROM employees e
FULL OUTER JOIN departments d 
    ON e.department_id = d.department_id
WHERE e.employee_id IS NULL 
   OR d.department_id IS NULL;


/* ----------------------------------------------------------------------------
   7. Cross Join
   Produces a Cartesian product—pairing every row from `employees` (21 rows) 
   with every row from `departments` (5 rows) = 105 total combinations.
   ---------------------------------------------------------------------------- */
SELECT 
    e.first_name,
    e.last_name,
    d.department_name AS potential_department
FROM employees e
CROSS JOIN departments d
ORDER BY e.employee_id, d.department_id;


/* ----------------------------------------------------------------------------
   8. Natural Join
   Implicitly joins tables based on all columns with the exact same name in 
   both tables (`department_id` exists in both `employees` and `departments`).
   ---------------------------------------------------------------------------- */
SELECT 
    employee_id,
    first_name,
    last_name,
    department_id,
    department_name,
    location
FROM employees
NATURAL JOIN departments
ORDER BY employee_id;
