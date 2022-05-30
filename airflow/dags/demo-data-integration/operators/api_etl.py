def crawl_job():
    import requests
    import pandas as pd
    import time

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def call_api(url: str):
        r = requests.get(url).json()
        time.sleep(1)
        return r

    # hàm lấy giá trị location
    def get_config_data(url):
        r = requests.get(url).json()
        data = r['data']['filter_configuration']['dynamic_filter_group_data']
        locations = []
        for location in data['locations']:
            locations.append(location['name'])
        return locations

    # hàm lấy tổng total count
    def get_total_count(location):
        url = f"https://shopee.vn/api/v4/search/search_items?&by=relevancy&limit=100&locations={location}&match_id={category}&newest={0 * 100}&order=desc&page_type=search"
        r = requests.get(url).json()
        print(r['total_count'])
        return r['total_count']

    r = requests.get('https://shopee.vn/api/v4/pages/get_category_tree').json()['data']['category_list']
    categories = []
    for category in r:
        for cate in category['children']:
            categories.append(cate['catid'])
    threads = []
    result = []
    category = categories[0]
    with ThreadPoolExecutor(max_workers=3) as executor:
        start_time = time.time()
        temp = 0
        get_config_url = f"https://shopee.vn/api/v4/search/search_filter_config?match_id={category}&page_type=search"
        locations = get_config_data(get_config_url)

        for location in locations:
            total = get_total_count(location)
            if total != 0:
                temp = temp + total
            for page in range(int(total / 100)):
                url = f"https://shopee.vn/api/v4/search/search_items?&by=relevancy&limit=100&locations={location}&match_id={category}&newest={page*100}&order=desc&page_type=search"
                threads.append(executor.submit(call_api, url))

        for task in as_completed(threads):
            try:
                if 'items' in task.result():
                    if task.result()['items'] is not None:
                        if len(task.result()['items']) != 0:
                            items = task.result()['items']
                            for item in items:
                                result.append(item['item_basic'])
            except Exception as e:
                print("ERROR")
                print(e.message)
                # print(task.result())

    df = pd.DataFrame(result)
    df = df.drop_duplicates('itemid').reset_index(drop=True)
    df.to_csv("/opt/airflow/dags/demo-data-integration/ShopeeData.csv")

def transform_data():
    import pandas as pd
    path = "/opt/airflow/dags/demo-data-integration/ShopeeData.csv"
    data = pd.read_csv(path)

    # transform
    mask = data["itemid"].apply(lambda x: isinstance(x, int))
    data = data[mask]

    chosen_columns = ["itemid", "shopid", "name", "currency", "stock", "status", "sold", "liked_count",\
                      "view_count", "catid", "brand", "cmt_count", "item_status", "price",\
                    "shop_location", "is_official_shop", "flag"]
    data = data[chosen_columns]
    data["name"] = data["name"].fillna("NoName").astype(str)
    data["currency"] = data["currency"].astype(str)
    data["brand"] = data["brand"].fillna("No Brand").astype(str)
    data["item_status"] = data["item_status"].fillna("normal").astype(str)
    data["shop_location"] = data["shop_location"].astype(str)
    data["is_official_shop"] = data["is_official_shop"].astype(bool)
    num_value = ["itemid", "shopid", "stock", "status", "sold", "liked_count", "view_count",\
                 "catid", "cmt_count", "price", "flag"]
    for item in num_value:
        data[item] = data[item].fillna(0)
    data.to_csv("/opt/airflow/dags/demo-data-integration/transformed_ShopeeData.csv", index=False)

def to_mysql():
    import os

    def read_dataframe(path):
        import pandas as pd
        try:
            return pd.read_csv(path)
        except:
            raise Exception()


    def setup_connection():
        import pymysql
        connection = pymysql.connect(host="host.docker.internal",\
                                    user="root",\
                                    password="longgiang2010",\
                                    db="etl_db")
        return connection

    conn = setup_connection()
    cursor = conn.cursor()

    cols = "`,`".join(["itemid", "shopid", "name", "currency", \
        "stock", "status", "sold", "liked_count", "view_count", "catid", "brand", \
            "cmt_count", "item_status", "price", "shop_location", "flag"])
    os.system("pwd")
    select_columns = ["itemid", "shopid", "name", "currency", \
        "stock", "status", "sold", "liked_count", "view_count", "catid", "brand", \
            "cmt_count", "item_status", "price", "shop_location", "flag"] 
    path = "/opt/airflow/dags/demo-data-integration/transformed_ShopeeData.csv"
    data = read_dataframe(path)
    data = data[select_columns]
    for i, row in data.iterrows():
        try:
            print(len(tuple(row)))
            sql = "INSERT INTO `data_api` (`" + cols +"`) VALUES("+" %s, "*(len(row)-1)+" %s)"
            print(sql)
            cursor.execute(sql,tuple(row))

            conn.commit()
        except:
            continue
    conn.close()
