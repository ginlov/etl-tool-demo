def hello():
    print("Hello World")

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

    cols = "`,`".join(["brand", "category", "mall", "product_name", \
        "views", "price", "rate1star", "rate2star", "rate3star", "rate4star", "rate5star", \
            "rating", "shop_name", "shope_rating", "response_rate", "ship_ontime"])
    os.system("pwd")
    select_columns = ["p_brand", "p_cate", "p_mall", "p_name", "p_number_reviews", "p_price",\
                      "p_rate1star", "p_rate2star", "p_rate3star", "p_rate4star", "p_rate5star",\
                      "p_rating", "s_name", "s_rating", "s_response_rate", "s_ship_ontime"]
    path = "/opt/airflow/dags/demo-data-integration/LazData.csv"
    data = read_dataframe(path)
    data = data[select_columns]
    for i, row in data.iterrows():
        print(len(tuple(row)))
        sql = "INSERT INTO `data_file` (`" + cols +"`) VALUES("+" %s, "*(len(row)-1)+" %s)"
        print(sql)
        cursor.execute(sql,tuple(row))

        conn.commit()
    conn.close()
