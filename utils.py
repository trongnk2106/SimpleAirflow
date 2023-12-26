import datetime
import os

import tqdm
import requests
from bs4 import BeautifulSoup, NavigableString, Tag
import re
article_type_dict = {
    0: "thoi-su",
    1: "du-lich",
    2: "the-gioi",
    3: "kinh-doanh",
    4: "khoa-hoc",
    5: "giai-tri",
    6: "the-thao",
    7: "phap-luat",
    8: "giao-duc",
    9: "suc-khoe",
    10: "doi-song"
}                        

def get_text_from_tag(tag):
    if isinstance(tag, NavigableString):
        return tag
                    
    # else if isinstance(tag, Tag):
    return tag.text

def extract_content(url):
    content = requests.get(url).content
    soup = BeautifulSoup(content, "html.parser")

    title = soup.find("h1", class_="title-detail") 
    if title == None:
        return None, None, None, None
    title = title.text

    # some sport news have location-stamp child tag inside description tag
    description = " ".join(list((get_text_from_tag(p) for p in soup.find("p", class_="description").contents)))
    paragraphs = " ".join(list((get_text_from_tag(p) for p in soup.find_all("p", class_="Normal"))))
    date = None
    try:
        date = get_text_from_tag(soup.find(class_=re.compile(r'\b(?:date-new|date)\b')))
    except Exception as e:
        print(e)
        print("ERROR DATE:",url)
    return title, description, paragraphs, date

def write_content(url):
    title, description, paragraphs, date = extract_content(url)
    # print(date.split(',')[1].strip())
    try:
        select_date = date.split(',')[1].strip()
        if select_date == datetime.datetime.now().strftime("%d/%m/%Y"):
            
            if title == None:
                return False
            if len(description)==2:
                description = description[0] + " - " + description[1]   
            content = {"title":title,"date":date,"description":description,"paragraphs":paragraphs}
            return content
        return None
    except:
        pass

def get_urls_of_type(article_type, total_pages=1):
    articles_urls = []
    article_type = article_type_dict[article_type]
    for i in tqdm.tqdm(range(1, total_pages+1)):
        content = requests.get(f"https://vnexpress.net/{article_type}-p{i}").content
        soup = BeautifulSoup(content, "html.parser")
        titles = soup.find_all(class_="title-news")
        if (len(titles) == 0):
            print(f"Couldn't find any news in the category {article_type} on page {i}")
            continue

        for title in titles:
            try:
                link = title.find_all("a")[0]
                articles_urls.append(str(link.get("href")))
            except:
                print(title)
            
    
    return articles_urls

# if __name__ == "__main__":
#     print(get_urls_of_type(4,1))
