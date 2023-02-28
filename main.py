import re
import time
import requests
from bs4 import BeautifulSoup
from database import record_model, all_offline, import_to_sql

ua_chrome = " ".join(["Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                      "AppleWebKit/537.36 (KHTML, like Gecko)",
                      "Chrome/108.0.0.0 Safari/537.36"])
headers = {"user-agent": ua_chrome}


def auto_import_to_sql():
    while True:
        time.sleep(1800)
        with open("check_tasks.txt", "r", encoding="utf-8") as file:
            content = file.read()
        check = True
        for index in range(1, 21):
            if not (f"OK-{index}" in content):
                check = False
                break
        if check:
            import_to_sql()
            with open("check_tasks.txt", "w", encoding="utf-8") as file:
                file.write("")


def get_product_info(session, product_link, place):
    response = session.get(url=product_link, headers=headers)
    bs_object = BeautifulSoup(response.content, "lxml")
    name = bs_object.h1.text.strip().replace('"', "'")
    date = bs_object.find(name="time", itemprop="datePublished").text.strip().replace('"', "'")
    model_id = bs_object.find(name="span", id="ProductID").text.strip()
    if len(model_id) == 0 or model_id is None:
        return "CONTINUE"
    model_id = int(model_id)
    price = bs_object.find(name="span", itemprop="price")["content"].replace(",", "")
    amount_images = len(bs_object.find(name="div", class_="es-carousel-wrapper").ul.find_all(name="li"))

    category_objects = bs_object.find(name="div", itemprop="breadcrumb").find_all(name="a")
    categories = list()
    for category_object in category_objects:
        categories.append(category_object.text.strip())
    categories = " > ".join(categories)
    categories = categories.replace('"', "'")

    keyword_objects = bs_object.find(name="span", id="FPKeywordEn").find_all(name='a')
    keywords = list()
    for keyword_object in keyword_objects:
        keywords.append(keyword_object.text.strip())
    keywords = ", ".join(keywords)
    keywords = keywords.replace('"', "'")

    description = bs_object.find(name="div", class_="descriptionContentParagraph").text
    description = str(description)
    description = description.replace('<div class="descriptionContentParagraph">', "").replace("</div>", "")
    description = description.replace("<br/>", "\n").replace('"', "'").replace("<h1>", "").replace("</h1>", "\n")
    description = description.replace("<p>", "").replace("</p>", "\n").replace("<h2>", "").replace("</h2>", "\n")

    native = bs_object.find(name="div", id="FPNativeFormat")
    if native is not None:
        native = native.div.text.replace("\n", "").replace("\t", "").strip()
    else:
        native = "-"
    formats = bs_object.find_all(name="div", id=re.compile(r"FPFormat\d"))
    formats = ", ".join(formats_element.text.replace("\n", "").replace("\t", "").strip() for formats_element in formats)
    polygons = int(bs_object.find(name="div", id="FPSpec_polygons").text.replace(",", "").replace("Polygons", ""))
    vertices = int(bs_object.find(name="div", id="FPSpec_vertices").text.replace(",", "").replace("Vertices", ""))

    certifications = bs_object.find(name="div", class_="CheckMateContent").text.strip().replace('"', "'")
    check_mate_pro = "-"
    check_mate_lite = "-"
    stem_cell = "-"
    if "CheckMate Pro Certified" in certifications:
        check_mate_pro = "+"
    if "CheckMate Lite Certified" in certifications:
        check_mate_lite = "+"
    if "StemCell Certified" in certifications:
        stem_cell = "+"

    author = bs_object.find(name="a", itemprop="creator").text.replace("by", "").strip().replace('"', "'")
    status = "Online"
    date_registration = bs_object.find(name="div", class_="ArtistSellInfo").text.strip().replace('"', "'")
    start_index = date_registration.index("Since")
    date_registration = date_registration[start_index:].replace('"', "'")

    record_model(model_id=model_id, name=name, date=date, price=price, status=status,
                 amount_images=amount_images, categories=categories, keywords=keywords, description=description,
                 native=native, formats=formats, polygons=polygons, vertices=vertices, stem_cell=stem_cell,
                 check_mate_pro=check_mate_pro, check_mate_lite=check_mate_lite, author=author,
                 date_registration=date_registration, place=place)


def get_product_links_from_page(page, place, session):
    url = f"https://www.turbosquid.com/Search/3D-Models?page_num={page}"
    response = session.get(url=url, headers=headers)
    bs_object = BeautifulSoup(response.content, "lxml")
    product_links = bs_object.find_all(name="a", class_="mouseover_fplink")
    for product_link in product_links:
        place += 1
        try:
            get_product_info(product_link=product_link["href"], place=place, session=session)
        except Exception as ex:
            print(f"[ERROR] {ex}")
            continue


def record_finish_flag(task_id):
    with open("finish-flag.txt", "a", encoding="utf-8", newline="\n") as file:
        file.write(f"OK-{task_id}")


def task(task_id):
    print(f"[INFO] Задача {task_id} запущена")
    session = requests.Session()
    while True:
        start_time = time.time()
        all_offline()
        url = "https://www.turbosquid.com/Search/3D-Models"
        response = session.get(url=url, headers=headers)
        bs_object = BeautifulSoup(response.content, "lxml")
        amount_pages = int(bs_object.find(name="span", id="ts-total-pages").text.strip())
        step_amount_pages = amount_pages // 20
        start_step = (task_id - 1) * step_amount_pages + 1
        stop_step = task_id * step_amount_pages + 1
        place = (start_step - 1) * 100
        for page in range(start_step, stop_step):
            print(f"[INFO] Собираем ссылки на модели со страницы {page}")
            start = time.time()
            try:
                get_product_links_from_page(page=page, place=place, session=session)
                place += 100
            except Exception as ex:
                print(f"[ERROR] {ex}")
                continue
            print(f"[TIME] Сбор 100 записей прошел за {time.time() - start}")
            print(f"[TIME] Собрано {page * 100} записей")
        record_finish_flag(task_id=task_id)
        stop_time = time.time()
        print(f"[INFO] Задача {task_id} завершена")
        print(f"[INFO] Время работы задачи: {stop_time - start_time}")


def run_task(task_id):
    while True:
        time.sleep(600)
        with open("check_tasks.txt", "r", encoding="utf-8") as file:
            content = file.read()
        if not (f"OK-{task_id}" in content):
            task(task_id=task_id)
