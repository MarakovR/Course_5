# Course_5

Проект для парсинга компаний и их вакансий с платформы ``HeadHunter`` через API и последующим сохранением в базу данных.
Используемая СУБД в проекте ``PostgreSQL``.

____

## База данных

### ERD диаграмма для базы данных

<a href="https://ibb.co/ykpKRYc"><img src="https://i.ibb.co/rp3qtcR/image.png" alt="image" border="0" /></a>

База данных имеет две таблицы:
1) companies (работадели)
2) vacancies (вакансии)

Отношения между таблицами one-to-many (один ко многим).


#### Таблица companies

Колонки:
* company_id (уникальный id компании, автоинкрементирующийся, тип serial с ограничением PRIMARY KEY, связана с колонкой company_id в таблице vacancies)
* company_name (название компании, тип varchar)
* description (описание компании, тип text)
* link (ссылка на компанию, тип varchar)
* url_vacancies (ссылка на вакансии компании, тип varchar)

#### Таблица vacancies

Колонки:
* vacancy_id (уникальный id вакансии, автоинкрементирующийся, тип serial с ограничением PRIMARY KEY)
* company_id (уникальный id компании, тип int, связан с колонкой company_id в таблице companies)
* title_vacancy (название вакансии, тип varchar)
* salary (зарплата, указанная в вакансии, тип int)
* link (ссылка на вакансию, тип varchar)
* description (описание вакансии, тип text)
* experience (требуемый опыт в вакансии, тип varchar)

____
### Как использовать данный проект?

* Склонировать репозиторий в IDE.

  В терминале ввести команду:
  ```
  git clone https://github.com/toir02/parser_hh
  ```
* Установить зависимости.

  В терминале ввести команду:
  ```
  pip install -r requirements.txt
  ```
* В модуле database.ini, в секции postgresql ввести свои данные

  1) host
    
     Ввести свой host, по умолчанию является localhost.
  2) user
     
     Ввести имя пользователя, используемого в СУБД.
  3) password
  
     Ввести пароль у пользователя в СУБД.
  4) port
 
     Ввести свой port, по умолчанию 5432. 

* В модуле main.py переменной ``database_name`` задать имя базы данных, которая в последствии будет использоваться.
* Класс DBManager

  У класса DBManager есть следующие методы:

  * get_companies_and_vacancies_count

    Метод для получения компаний и количества вакансий у каждой компании.

  * get_all_vacancies

    Метод для получения названии компании, названии вакансии, зарплату и ссылку на вакансию.

  * get_avg_salary
 
    Метод для получения названии компании и средней зарплаты у компании по всем вакансиям.

  * get_vacancies_wth_highest_salary
 
    Метод для получения всех вакансий, у которых зарплата выше средней.

  * get_vacancies_with_keyword
 
    Метод для получения вакансий, по используемому ключевому слову в названии вакансии.
    Необходимо задать ключевое слово переменной, чтобы вызвать этот метод.
    
    Пример:
    ```python
    keyword = input('Введите ключевое слово для поиска вакансий: ').lower()
    ```


  Примеры вызова созданного экземпляра класса DBManager:
    ```python
    db_manager.get_companies_and_vacancies_count()
    db_manager.get_all_vacancies()
    db_manager.get_avg_salary()
    db_manager.get_vacancies_wth_highest_salary()
    db_manager.get_vacancies_with_keyword(keyword)
    ```
