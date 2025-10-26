import mysql.connector
from mysql.connector import errorcode
from faker import Faker
from tqdm import tqdm
import random
from datetime import date, datetime, timedelta

DB_CONFIG = {
    "user": "root",
    "password": "MySQL_Student0303", 
    "host": "127.0.0.1",
    "port": 3306,
}
DB_NAME = "intl_exchange_db"


NUM_SPONSORS    = 1_000_000
NUM_PROGRAMS    = 1_000_000
NUM_ENROLLMENTS = 1_000_000


BATCH_SIZE = 10_000


fake = Faker()
COUNTRIES = ["US","GB","DE","FR","UA","PL","ES","IT","NL","SE"]
SPONSOR_TYPES = ["government","ngo","university","foundation","company"]
FIELDS = ["education","research","culture","sport","volunteering","arts"]
ROLES = ["student","researcher","teacher","volunteer","athlete","artist"]
STATUSES = ["applied","accepted","active","completed","withdrawn"]
WELL_KNOWN_SPONSORS = ["Erasmus+","Fulbright Program","DAAD","Chevening","Erasmus Mundus"]


def create_database(cursor):
    try:
        cursor.execute(f"CREATE DATABASE {DB_NAME} DEFAULT CHARACTER SET utf8mb4")
        print(f"База даних '{DB_NAME}' створена.")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DB_CREATE_EXISTS:
            print(f"База даних '{DB_NAME}' вже існує.")
        else:
            raise

def create_tables(cursor):
    TABLES = {}

    TABLES["sponsors"] = (
        "CREATE TABLE `sponsors` ("
        "  `sponsor_id` BIGINT AUTO_INCREMENT PRIMARY KEY,"
        "  `sponsor_name` VARCHAR(200) NOT NULL,"
        "  `sponsor_type` ENUM('government','ngo','university','foundation','company') NOT NULL,"
        "  `hq_country`   CHAR(2) NOT NULL,"
        "  `created_at`   DATETIME NOT NULL,"
        "  KEY `idx_sponsors_type_country` (`sponsor_type`,`hq_country`)"
        ") ENGINE=InnoDB"
    )

    TABLES["programs"] = (
        "CREATE TABLE `programs` ("
        "  `program_id`   BIGINT AUTO_INCREMENT PRIMARY KEY,"
        "  `sponsor_id`   BIGINT NOT NULL,"
        "  `program_name` VARCHAR(250) NOT NULL,"
        "  `field_domain` ENUM('education','research','culture','sport','volunteering','arts') NOT NULL,"
        "  `host_country` CHAR(2) NOT NULL,"
        "  `start_date`   DATE NOT NULL,"
        "  `end_date`     DATE NOT NULL,"
        "  CONSTRAINT `fk_program_sponsor` FOREIGN KEY (`sponsor_id`) REFERENCES `sponsors`(`sponsor_id`),"
        "  KEY `idx_programs_sponsor` (`sponsor_id`),"
        "  KEY `idx_programs_field_domain` (`field_domain`,`program_id`)"
        ") ENGINE=InnoDB"
    )

    TABLES["enrollments"] = (
        "CREATE TABLE `enrollments` ("
        "  `enrollment_id`  BIGINT AUTO_INCREMENT PRIMARY KEY,"
        "  `program_id`     BIGINT NOT NULL,"
        "  `participant_id` BIGINT NOT NULL,"
        "  `full_name`      VARCHAR(180) NOT NULL,"
        "  `home_country`   CHAR(2) NOT NULL,"
        "  `host_country`   CHAR(2) NOT NULL,"
        "  `home_university` VARCHAR(200),"
        "  `host_university` VARCHAR(200),"
        "  `role`           ENUM('student','researcher','teacher','volunteer','athlete','artist') NOT NULL,"
        "  `status`         ENUM('applied','accepted','active','completed','withdrawn') NOT NULL,"
        "  `start_date`     DATE NOT NULL,"
        "  `end_date`       DATE NOT NULL,"
        "  CONSTRAINT `fk_enroll_program` FOREIGN KEY (`program_id`) REFERENCES `programs`(`program_id`),"
        "  KEY `idx_enroll_program_status` (`program_id`,`status`,`role`,`start_date`)"
        ") ENGINE=InnoDB"
    )

    for name, ddl in TABLES.items():
        try:
            print(f"Створення таблиці {name}... ", end="")
            cursor.execute(ddl)
            print("OK")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("вже існує.")
            else:
                print(f"\n{err.msg}")
                raise

def gen_sponsor_row():
    if random.randint(1, 250) == 1:
        sponsor_name = random.choice(WELL_KNOWN_SPONSORS)
    else:
        sponsor_name = f"{fake.company()} Foundation"
    sponsor_type = random.choice(SPONSOR_TYPES)
    hq_country = random.choice(COUNTRIES)
    created_at = fake.date_time_between(start_date="-10y", end_date="now")
    return (sponsor_name, sponsor_type, hq_country, created_at)

def gen_program_row(max_sponsor_id):
    sponsor_id = random.randint(1, max_sponsor_id)
    base = random.choice(["International Exchange", "Mobility", "Grant", "Fellowship"])
    program_name = f"{base} Program {fake.unique.random_int(min=1, max=10_000_000)}"
    field_domain = random.choice(FIELDS)
    host_country = random.choice(COUNTRIES)
    start = fake.date_between(date(2023,1,1), date(2024,12,31))
    end = start + timedelta(days=90)
    return (sponsor_id, program_name, field_domain, host_country, start, end)

def gen_enrollment_row(max_program_id):
    program_id = random.randint(1, max_program_id)
    participant_id = fake.unique.random_int(min=1, max=5_000_000_000)
    full_name = fake.name()
    home_country = random.choice(COUNTRIES)
    host_country = random.choice(COUNTRIES)
    home_univ = f"Home Univ #{fake.random_int(1, 50000)}"
    host_univ = f"Host Univ #{fake.random_int(1, 50000)}"
    role = random.choice(ROLES)
    status = random.choice(STATUSES)
    start = fake.date_between(date(2023,1,1), date(2023,12,31))
    end = start + timedelta(days=90)
    return (program_id, participant_id, full_name, home_country, host_country,
            home_univ, host_univ, role, status, start, end)

def bulk_insert(cursor, sql, row_factory, total, desc):
    left = total
    with tqdm(total=total, desc=desc, unit="row") as pbar:
        while left > 0:
            batch = min(BATCH_SIZE, left)
            data = [row_factory() for _ in range(batch)]
            cursor.executemany(sql, data)
            left -= batch
            pbar.update(batch)

if __name__ == "__main__":
    cnx = None
    try:
        # 1) Підключення
        cnx = mysql.connector.connect(**DB_CONFIG)
        cur = cnx.cursor()

        # 2) Створити/вибрати БД
        create_database(cur)
        cur.execute(f"USE {DB_NAME}")

        # 3) Створити таблиці
        create_tables(cur)
        cnx.commit()

        print("\n--- Наповнення таблиць ---")

        # 4) Sponsors (≥ 1 млн)
        sql_sponsors = (
            "INSERT INTO sponsors (sponsor_name, sponsor_type, hq_country, created_at) "
            "VALUES (%s, %s, %s, %s)"
        )
        bulk_insert(cur, sql_sponsors, gen_sponsor_row, NUM_SPONSORS, "Sponsors")
        cnx.commit()

        # ID-діапазон для FK
        cur.execute("SELECT MAX(sponsor_id) FROM sponsors")
        (max_sponsor_id,) = cur.fetchone()

        # 5) Programs (≥ 1 млн)
        sql_programs = (
            "INSERT INTO programs (sponsor_id, program_name, field_domain, host_country, start_date, end_date) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )
        bulk_insert(cur, sql_programs, lambda: gen_program_row(max_sponsor_id), NUM_PROGRAMS, "Programs")
        cnx.commit()

        cur.execute("SELECT MAX(program_id) FROM programs")
        (max_program_id,) = cur.fetchone()

        # 6) Enrollments (≥ 1 млн)
        sql_enroll = (
            "INSERT INTO enrollments (program_id, participant_id, full_name, home_country, host_country, "
            "home_university, host_university, role, status, start_date, end_date) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        bulk_insert(cur, sql_enroll, lambda: gen_enrollment_row(max_program_id), NUM_ENROLLMENTS, "Enrollments")
        cnx.commit()

        print("\n🎉 Готово! Дані згенеровано.")
        for tbl in ["sponsors","programs","enrollments"]:
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            (cnt,) = cur.fetchone()
            print(f"  - {tbl}: {cnt:,}")

    except mysql.connector.Error as err:
        print(f"Помилка MySQL: {err}")
    except Exception as e:
        print(f"Помилка: {e}")
    finally:
        if cnx and cnx.is_connected():
            cur.close()
            cnx.close()
            print("Підключення до MySQL закрито.")
