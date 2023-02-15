import pymysql
from pymysql import cursors
from config import port, host, db_name, db_username, password
import datetime
import pathlib


def database(query):
    try:
        connection = pymysql.connect(port=port, host=host, user=db_username, password=password,
                                     database=db_name, cursorclass=cursors.DictCursor)
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            connection.commit()
            return result
        except Exception as ex:
            print(f"Something Wrong: {ex}")
            return "Error"
        finally:
            connection.close()
    except Exception as ex:
        print(f"Connection was not completed because {ex}")
        return "Error"


def database_all(query):
    try:
        connection = pymysql.connect(port=port, host=host, user=db_username, password=password,
                                     database=db_name, cursorclass=cursors.DictCursor)
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            connection.commit()
            return result
        except Exception as ex:
            print(f"Something Wrong: {ex}")
            return "Error"
        finally:
            connection.close()
    except Exception as ex:
        print(f"Connection was not completed because {ex}")
        return "Error"


def create_table():
    query = """CREATE TABLE models (`parsing_date` VARCHAR(15), `id` INT UNIQUE, 
              `name` VARCHAR(200), `date` VARCHAR(20), `price` VARCHAR(50), `status` VARCHAR(20), `amount_images` INT, 
              `categories` VARCHAR(1000), `keywords` VARCHAR(1000), `description` TEXT, `native` VARCHAR(200), 
              `formats` VARCHAR(1500), `polygons` INT, `vertices` INT, `stem_cell` VARCHAR(1), 
              `check_mate_pro` VARCHAR(1), `check_mate_lite` VARCHAR(1), `author` VARCHAR(50), 
              `date_registration` VARCHAR(20), `place` INT);"""
    result = database(query=query)
    if result != "Error":
        print("[INFO] Таблица models успешно создана в базе данных")
    else:
        print('[ERROR] Проверьте конфигурации базы данных')


def all_offline():
    query = """UPDATE models SET `status` = '-';"""
    database(query=query)


def record_model(model_id, name, date, price, status, amount_images, categories,
                 keywords, description, native, formats, polygons, vertices, stem_cell,
                 check_mate_pro, check_mate_lite, author, date_registration, place):
    query = f"SELECT * FROM models WHERE `id` = {model_id};"
    check = database(query=query)
    parsing_date = str(datetime.date.today())
    if check is not None:
        query = f"""UPDATE models SET `parsing_date` = '{parsing_date}', `name` = "{name}", `date` = "{date}", 
                    `price` = "{price}",
                    `status` = "{status}", `amount_images` = {amount_images}, `categories` = "{categories}",
                    `keywords` = "{keywords}", `description` = "{description}", 
                    `native` = "{native}", `formats` = "{formats}", `polygons` = {polygons}, 
                    `vertices` = {vertices},`stem_cell` = "{stem_cell}", 
                    `check_mate_pro` = "{check_mate_pro}", `check_mate_lite` = "{check_mate_lite}", 
                    `author` = "{author}", `date_registration` = "{date_registration}", `place` = "{place}"
                    WHERE `id` = {model_id};"""
        successful_text = f"[INFO] Модель {name} успешно перезаписана в базу данных"
    else:
        query = f"""INSERT INTO models VALUES ('{parsing_date}', {model_id}, "{name}", "{date}", 
                    "{price}", "{status}", {amount_images}, "{categories}", "{keywords}", "{description}", "{native}", 
                    "{formats}", "{polygons}", "{vertices}", "{stem_cell}", "{check_mate_pro}", "{check_mate_lite}", 
                    "{author}", "{date_registration}", "{place}");"""
        successful_text = f"[INFO] Модель {name} успешно записана в базу данных"
    result = database(query=query)
    if result != "Error":
        print(successful_text)


def import_to_sql():
    print("[INFO] Создаем дамп базы данных")
    today = datetime.date.today()
    result = list()
    path = pathlib.Path("results", f"{today}.sql")
    with open(path, "w") as file:
        start_query = "DROP TABLE IF EXISTS `models`;\n" + "CREATE TABLE models (`parsing_date` VARCHAR(15), `id` INT UNIQUE,  `name` VARCHAR(200), `date` VARCHAR(20), `price` VARCHAR(50), `status` VARCHAR(20),  `amount_images` INT, `categories` VARCHAR(1000), `keywords` VARCHAR(1000),  `description` TEXT, `native` VARCHAR(200), `formats` VARCHAR(1500), `polygons` INT,  `vertices` INT, `stem_cell` VARCHAR(1), `check_mate_pro` VARCHAR(1),  `check_mate_lite` VARCHAR(1), `author` VARCHAR(50),  `date_registration` VARCHAR(20), `place` INT);\n"
        file.write(start_query)
    data = database_all(query="SELECT * FROM models;")
    for element in data:
        model_id = element["id"]
        name = element["name"]
        date = element["date"]
        price = element["price"]
        status = element["status"]
        amount_images = element["amount_images"]
        categories = element["categories"]
        keywords = element["keywords"]
        description = element["description"]
        native = element["native"]
        formats = element["formats"]
        polygons = element["polygons"]
        vertices = element["vertices"]
        stem_cell = element["stem_cell"]
        check_mate_pro = element["check_mate_pro"]
        check_mate_lite = element["check_mate_lite"]
        author = element["author"]
        date_registration = element["date_registration"]
        place = element["place"]
        parsing_date = element["parsing_date"]
        query = f"""INSERT INTO models VALUES ('{parsing_date}', {model_id}, "{name}", "{date}", 
                    "{price}", "{status}", {amount_images}, "{categories}", "{keywords}", "{description}", "{native}", 
                    "{formats}", "{polygons}", "{vertices}", "{stem_cell}", "{check_mate_pro}", "{check_mate_lite}", 
                    "{author}", "{date_registration}", "{place}");"""
        result.append(query)
    result = "\n".join(result)
    with open(path, "a") as file:
        file.write(result)
    print(f"[INFO] Дамп базы данных успешно создан: {datetime.date.today()}")


if __name__ == "__main__":
    create_table()
