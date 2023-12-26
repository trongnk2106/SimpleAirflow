from pymongo import MongoClient

class DB:
    def __init__(self, username, password, cluster_name, database_name):
        self.username = username
        self.password = password
        self.cluster_name = cluster_name
        self.database_name = database_name
        self.client = MongoClient(f"mongodb+srv://{username}:{password}@{cluster_name}.mongodb.net/{database_name}?retryWrites=true&w=majority")

        self.db = self.client[database_name]
        self.collection = self.db["newsdb"]
        self.collection.create_index([("date", "text")])

    @staticmethod   
    def get_instance():
        return DB("trongnt2002", "trongnt2002", "admin.4xteipz", "cs336")
        # return DB("trongnt2002", "trongnt2002", "simplecrawler.y6n3eao", "store_crawler")
    
    
# if __name__ == "__main__":
    # data = {"title": "title", "date": "date", "description": "description", "paragraphs": "paragraphs"}
    # DB.get_instance().collection.update_one({"date" : "22/12/2023"},{"$push": {"results": data}}, upsert=True)
  