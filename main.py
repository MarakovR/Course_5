from utils import get_vacancies, create_database, save_data_to_database, DBManager
from config import config


def main():
    company = [78638,  # Тинькофф
               84585,  # Авито
               3529,  # Сбер
               2523,  # М.Видео-Эльдорадо
               240410,  # ООО Maxima
               1122462,  # Skyeng
               4988808,  # ООО Ларссон
               9188643,  # ООО Грин-Апи
               4023,  # Райффайзен Банк
               1035394  # Красное & Белое
               ]

    params = config()
    database_name = 'hh'

    data = get_vacancies(company)
    create_database(database_name, params)
    save_data_to_database(data, database_name, params)

    db_manager = DBManager(database_name, params)

    test = db_manager.get_all_vacancies()
    print(test)


if __name__ == '__main__':
    main()
