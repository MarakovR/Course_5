import requests
from typing import Any
import psycopg2


def get_vacancies(companies):
    """ Получение списка вакансий заданной компании """
    employers = []
    for company in companies:
        url = f'https://api.hh.ru/employers/{company}'
        company_response = requests.get(url).json()
        vacancy_response = requests.get(company_response['vacancies_url']).json()
        print(f'Получение данных с {company_response["name"]}...')
        employers.append({
            'company': company_response,
            'vacancies': vacancy_response['items']
        })
    return employers


def reform_salary(dict_salary):
    if dict_salary is None:
        salary = None
    elif dict_salary['from'] is None:
        salary = dict_salary["to"]
    elif dict_salary['to'] is None:
        salary = dict_salary["from"]
    else:
        salary = round((dict_salary["to"] + dict_salary["from"]) / 2)
    return salary


def create_database(database_name: str, params: dict):
    """ Создание базы данных и таблиц для храниения данных о вакансиях"""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f'DROP DATABASE {database_name}')
    cur.execute(f'CREATE DATABASE {database_name}')

    cur.close()
    conn.close()

    connection = psycopg2.connect(database=database_name, **params)

    with connection.cursor() as cursor:
        cursor.execute('CREATE TABLE companies('
                       'company_id serial PRIMARY KEY,'
                       'company_name varchar(50) NOT NULL,'
                       'description text,'
                       'link varchar(200) NOT NULL)')

        cursor.execute('CREATE TABLE vacancies('
                       'vacancy_id serial PRIMARY KEY,'
                       'company_id int REFERENCES companies (company_id) NOT NULL,'
                       'title_vacancy varchar(150) NOT NULL,'
                       'link varchar(200) NOT NULL,'
                       'salary varchar(20),'
                       'description text)')

    connection.commit()
    connection.close()


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict):
    """ Сохранение данных о канал и видео в базу данных"""
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for companies in data:
            company_data = companies['company']
            cur.execute(
                """
                INSERT INTO companies (company_name, description, link)
                VALUES (%s, %s, %s)
                RETURNING company_id
                """,
                (company_data['name'], company_data['description'], company_data['alternate_url'])
            )
            company_id = cur.fetchone()[0]

            for vacancy in companies['vacancies']:
                salary = reform_salary(vacancy['salary'])
                cur.execute(
                    """
                    INSERT INTO vacancies (company_id, title_vacancy, link, salary, description)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (company_id, vacancy['name'], vacancy['alternate_url'], salary,
                     vacancy['snippet']['responsibility'])
                )

    conn.commit()
    conn.close()


class DBManager:

    def __init__(self, database_name: str, params: dict):
        self.database_name = database_name
        self.params = params

    def get_companies_and_vacancies_count(self):
        """ получает список всех компаний и количество вакансий у каждой компании """
        connection = psycopg2.connect(database=self.database_name, **self.params)
        with connection.cursor() as cursor:
            cursor.execute('SELECT company_name, COUNT(vacancy_id) '
                           'FROM companies '
                           'JOIN vacancies USING (company_id) '
                           'GROUP BY company_name;')

            data = cursor.fetchall()
        connection.close()
        return data

    def get_all_vacancies(self):
        """ получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на
        вакансию"""
        connection = psycopg2.connect(database=self.database_name, **self.params)
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT company_name, title_vacancy, salary, vacancies.link '
                'FROM companies '
                'JOIN vacancies USING (company_id);')

            data = cursor.fetchall()
        connection.close()
        return data

    def get_avg_salary(self):
        """ получает среднюю зарплату по вакансиям """
        connection = psycopg2.connect(database=self.database_name, **self.params)
        with connection.cursor() as cursor:
            cursor.execute('SELECT company_name, round(AVG(salary::int)) AS average_salary '
                           'FROM companies '
                           'JOIN vacancies USING (company_id) '
                           'GROUP BY company_name;')

            data = cursor.fetchall()
        connection.close()
        return data

    def get_vacancies_with_higher_salary(self):
        """ получает список всех вакансий, у которых зарплата выше средней по всем вакансиям """
        connection = psycopg2.connect(database=self.database_name, **self.params)
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM vacancies '
                           'WHERE salary::int > (SELECT AVG(salary::int) FROM vacancies);')

            data = cursor.fetchall()
        connection.close()
        return data

    def get_vacancies_with_keyword(self, keyword):
        """ получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “python” """
        connection = psycopg2.connect(database=self.database_name, **self.params)
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT * 
                FROM vacancies
                WHERE lower(title_vacancy) LIKE '%{keyword}%'
                OR lower(title_vacancy) LIKE '%{keyword}'
                OR lower(title_vacancy) LIKE '{keyword}%'""")

            data = cursor.fetchall()
        connection.close()
        return data
