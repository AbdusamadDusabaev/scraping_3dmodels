import re
import time
import requests
from bs4 import BeautifulSoup
from database import record_model, all_offline, import_to_sql


ua_chrome = " ".join(["Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                      "AppleWebKit/537.36 (KHTML, like Gecko)",
                      "Chrome/108.0.0.0 Safari/537.36"])
headers = {"user-agent": ua_chrome}


def get_product_info(session, product_link, place):
    response = session.get(url=product_link, headers=headers)
    bs_object = BeautifulSoup(response.content, "lxml")
    name = bs_object.h1.text.strip().replace('"', "'")
    print(f"[INFO] Записываем модель {name} ({product_link}) в базу данных")
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
        get_product_info(product_link=product_link["href"], place=place, session=session)


def main():
    print("[INFO] Программа запущена")
    session = requests.Session()
    while True:
        start_time = time.time()
        all_offline()
        url = "https://www.turbosquid.com/Search/3D-Models"
        response = session.get(url=url, headers=headers)
        bs_object = BeautifulSoup(response.content, "lxml")
        amount_pages = int(bs_object.find(name="span", id="ts-total-pages").text.strip())
        place = 0
        for page in range(1, amount_pages + 1):
            print(f"[INFO] Собираем ссылки на модели со страницы {page}")
            start = time.time()
            get_product_links_from_page(page=page, place=place, session=session)
            place += 100
            print(f"[TIME] Сбор 100 записей прошел за {time.time() - start}")
        import_to_sql()
        stop_time = time.time()
        print("[INFO] Программа завершена")
        print(f"[INFO] Время работы программы: {stop_time - start_time}")


if __name__ == "__main__":
    main()
