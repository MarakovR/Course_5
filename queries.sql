DROP DATABASE;
CREATE DATABASE;

CREATE TABLE companies(
company_id serial PRIMARY KEY,
company_name varchar(50) NOT NULL,
description text,
link varchar(200) NOT NULL);

CREATE TABLE vacancies(
vacancy_id serial PRIMARY KEY,
company_id int REFERENCES companies (company_id) NOT NULL,
title_vacancy varchar(150) NOT NULL,
link varchar(200) NOT NULL,
salary varchar(20),
description text);

INSERT INTO companies (company_name, description, link)
VALUES (%s, %s, %s)
RETURNING company_id;

INSERT INTO vacancies (company_id, title_vacancy, link, salary, description)
VALUES (%s, %s, %s, %s, %s);

SELECT company_name, COUNT(vacancy_id)
FROM companies
JOIN vacancies USING (company_id)
GROUP BY company_name;

SELECT company_name, title_vacancy, salary, vacancies.link
FROM companies
JOIN vacancies USING (company_id);

SELECT company_name, round(AVG(salary::int)) AS average_salary
FROM companies
JOIN vacancies USING (company_id)
GROUP BY company_name;

SELECT * FROM vacancies
WHERE salary::int > (SELECT AVG(salary::int) FROM vacancies);

SELECT * FROM vacancies
WHERE lower(title_vacancy) LIKE '%keyword%'
OR lower(title_vacancy) LIKE '%keyword'
OR lower(title_vacancy) LIKE 'keyword%'

