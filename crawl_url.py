import argparse
import datetime
import tqdm
from utils import get_urls_of_type, write_content
from db import DB

collection = DB.get_instance().collection

def crawl_urls(urls):
    index_len = len(str(len(urls)))
           
    contents = []
    with tqdm.tqdm(total=len(urls)) as pbar:
        for i, url in enumerate(urls):
            file_index = str(i+1).zfill(index_len)
            output_fpath = "".join(["/url_", file_index, ".txt"])
            content = write_content(url)
            if not content:
                print(url)
            else:
                contents.append(content)
            pbar.update(1)
    #contents = list({"title":title,"date":date,"description":description,"paragraphs":list(paragraphs)})
    return contents

def main(article_type=4, total_pages=1):
    return crawl_urls(get_urls_of_type(article_type, total_pages))
    

def run():
    
    a = main(article_type=4, total_pages=1)
    collection.update_many({"date" : datetime.datetime.now().strftime("%d/%m/%Y")}, {"$set": {"data" :a}}, upsert=True)
    # return time minute now 
    return datetime.datetime.now().minute
    
    
    # return datetime.datetime.now().strftime("%H")
    # print(a)
    # with open('./test.txt', 'w', encoding='utf-8') as f:
    #     for i in a:
    #         f.write(str(i))
    #         f.write('\n')
    
    # #write jsonl file
    # import json
    # with open('test.jsonl', 'w', encoding='utf-8') as json_file:
    #     for i in a:
    #         json.dump(i, json_file, ensure_ascii=False)
    #         json_file.write('\n')
# if __name__ == "__main__":
#     # print(datetime.datetime.now().strftime("%d/%m/%Y"))
#     a = main(article_type=4, total_pages=1)
#     # print(a)
#     with open('./test.txt', 'w', encoding='utf-8') as f:
#         for i in a:
#             f.write(str(i))
#             f.write('\n')
    
#     #write jsonl file
#     import json
#     with open('test.jsonl', 'w', encoding='utf-8') as json_file:
#         for i in a:
#             json.dump(i, json_file, ensure_ascii=False)
#             json_file.write('\n')
   
    
    # for i in a:
    #     print(i)
    #     break
'''
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
'''