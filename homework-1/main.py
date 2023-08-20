"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv


class EnginPG:

    def __init__(self):
        bd_password = input("Введите пароль для подключение к БД: ")
        self.pg_conf_host = "localhost"
        self.pg_conf_database = "north"
        self.pg_conf_user = "postgres"
        self.pg_conf_password = bd_password
        self.__path_employees_data = "./north_data/employees_data.csv"
        self.__path_customers_data = "./north_data/customers_data.csv"
        self.__path_orders_data = "./north_data/orders_data.csv"

    @property
    def path_employees_data(self) -> str:
        return self.__path_employees_data

    @property
    def path_customers_data(self) -> str:
        return self.__path_customers_data

    @property
    def path_orders_data(self) -> str:
        return self.__path_orders_data

    def save_pg_employees(self, employees: list):
        """Запись данных employees в БД таблицы employees"""
        i = 0
        conn = psycopg2.connect(host=self.pg_conf_host, database=self.pg_conf_database, user=self.pg_conf_user,
                                password=self.pg_conf_password)
        try:
            with conn:
                with conn.cursor() as cur:
                    for employee in employees:
                        i += 1
                        cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                    [i] + list(employee.values()))
        finally:
            conn.close()

    def save_pg_customers(self, customers: list):
        """Запись данных customers в БД таблицы customers"""
        conn = psycopg2.connect(host=self.pg_conf_host, database=self.pg_conf_database, user=self.pg_conf_user,
                                password=self.pg_conf_password)
        try:
            with conn:
                with conn.cursor() as cur:
                    for customer in customers:
                        cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", list(customer.values()))
                    pass
        finally:
            conn.close()

    def save_pg_orders(self, orders: list):
        """Запись данных orders в БД таблицы orders"""
        conn = psycopg2.connect(host=self.pg_conf_host, database=self.pg_conf_database, user=self.pg_conf_user,
                                password=self.pg_conf_password)
        try:
            with conn:
                with conn.cursor() as cur:
                    for order in orders:
                        cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", list(order.values()))
                    pass
        finally:
            conn.close()

    @staticmethod
    def open_csv(file_path: str) -> list:
        """Получение списка объектов"""
        data_list = []
        with open(file_path, newline='') as csvfile:
            spamreader = csv.DictReader(csvfile)
            for row in spamreader:
                data_list.append(row)
        return data_list


def pg_action():
    pg = EnginPG()
    pg.save_pg_employees(pg.open_csv(pg.path_employees_data))
    pg.save_pg_customers(pg.open_csv(pg.path_customers_data))
    pg.save_pg_orders(pg.open_csv(pg.path_orders_data))


if __name__ == "__main__":
    pg_action()
