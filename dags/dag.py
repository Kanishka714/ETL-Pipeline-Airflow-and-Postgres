#dag - directed acyclic graph
#tasks: 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators: Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from airflow import DAG 
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#fetch amazon data and then clean it
headers = {
    "User-Agent": ""
}

def get_amazon_data_books(num_books, ti):
    base_url = f"https://www.amazon.co.uk/s?k=financial+books"

    books = []
    seen_titles = set()

    page = 1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            book_container = soup.find_all('div', {"class": "s-result-item"})

            for book in book_container:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("a", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})
                description = book.find("span", {"class": "a-size-base"})


                if title and author and price and rating and description:
                    book_title = title.text.strip()

                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "Title": title.text.strip(),
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip() if rating else "N/A", #some books may not have ratings and it causes troubles when inserting data into database
                            "Description": description.text.strip()
                        })
                
            page += 1
        else:
            break
    
    books = books[:num_books]
    df = pd.DataFrame(books)
    df.drop_duplicates(inplace=True)
    ti.xcom_push(key="books_data", value=df.to_dict('records'))


def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key="books_data", task_ids=["fetch_amazon_books"]) [0]
    if not book_data:
        raise ValueError("No data available to store in Postgres")
    
    postgres_hook = PostgresHook(postgres_conn_id="books_connection")
    insert_query = """
        INSERT INTO books (title, authors, price, rating, description)
        VALUES (%s, %s, %s, %s, %s)
    """ 
    
    for book in book_data:
        # Execute the insert query with parameters
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating'], book['Description']))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch and store Amazon books data',
    schedule_interval=timedelta(days=1),
)


fetch_amazon_books = PythonOperator(
    task_id='fetch_amazon_books',
    python_callable=get_amazon_data_books,
    op_args=[50],
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255),
        authors VARCHAR(255),
        price VARCHAR(255),
        rating VARCHAR(255),
        description TEXT
    )
    """,
    dag=dag,
)


insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

fetch_amazon_books >> create_table_task >> insert_data_task