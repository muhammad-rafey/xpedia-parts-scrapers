import psycopg2
import requests
import json
import time
import uuid
import pdb
from datetime import datetime

# Database connection details
DB_HOST = "localhost"
DB_NAME = "scraper_db"
DB_USER = "scraper_user"
DB_PASSWORD = "sheri123"

# Connect to the PostgreSQL database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# Create a job entry in the Jobs table
def create_job(conn, scraper_name):
    job_id = str(uuid.uuid4())
    start_time = datetime.now()
    status = "started"
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO Jobs (job_id, scraper_name, start_time, status)
            VALUES (%s, %s, %s, %s)
        """, (job_id, scraper_name, start_time, status))
        conn.commit()
        cur.close()
        return job_id
    except Exception as e:
        print(f"Error creating job entry: {e}")
        return None

# Update the job entry with stats
def update_job(conn, job_id, status, total_products, end_time, execution_time):
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE Jobs
            SET status = %s, total_products = %s, end_time = %s, execution_time = %s
            WHERE job_id = %s
        """, (status, total_products, end_time, execution_time, job_id))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error updating job entry: {e}")

# Save scraped products to the Products table
def save_products(conn, job_id, products):
    try:
        cur = conn.cursor()
        for product in products:
            product_id = str(uuid.uuid4())
            scraped_at = datetime.now()
            cur.execute("""
                INSERT INTO Products (product_id, job_id, data, scraped_at)
                VALUES (%s, %s, %s, %s)
            """, (product_id, job_id, json.dumps(product), scraped_at))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error saving products: {e}")

# Fetch data from the API with retries
def fetch_with_retries(url, headers, retries=3, delay=5):
    for i in range(retries):
        try:
            proxy = {
                "http": "http://customer-qwjkdb_3dQua-cc-US:6D3PN=Csd_xiBi_@pr.oxylabs.io:7777",
                "https": "http://customer-qwjkdb_3dQua-cc-US:6D3PN=Csd_xiBi_@pr.oxylabs.io:7777"
            }
            # response = requests.get(url, headers=headers, proxies=proxy)
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response
        except requests.exceptions.RequestException as e:
            print(f"Attempt {i + 1} failed: {e}")
            time.sleep(delay)
    return None

# Scraper function
def fetch_all_products(api_url, take=100, job_id=None, conn=None):
    skip = 0
    headers = {

        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Expires': 'Sat, 01 Jan 2000 00:00:00 GMT',
        'If-Modified-Since': '0',
        'XvPW5hYbpt-f': 'A6yhkmyVAQAA3o7djlF0WkTN_xeqGPBjvVAEeD1HOueo4dhxkw6uqZaUjmhSAc_0R1CcuIhSwH9eCOfvosJeCA==',
        'XvPW5hYbpt-b': '13dvde',
        'XvPW5hYbpt-c': 'AMDJgWyVAQAAJqOmmidWELB4jhty2tE7M-zjOe8firYnLgA--hep0qwWP_8Z',
        'XvPW5hYbpt-d': 'ADaAhIDBCKGBgQGAAYIQgISigaIAwBGAzPpCxg_32ocxnsD_CAAXqdKsFj__Gf_____6SJXVAhQd-DNiVTjzFsc17PziE0M',
        'XvPW5hYbpt-z': 'q',
        'XvPW5hYbpt-a': '2jg6a3aATsaGTGinIfJDxNQl_tz=roFRaxdqAEzzN9Hxz=PFxauaRaKspmIdmbgTlmm2UuH=dbx_T_V2tfpdVpLPHMXsxYSJBi2ATXo6ZeZ1Xs1ayjpAa88k=-zzfFtl=MUfFtob73hqLuVNhP93Duch_ii8xYFY64eO_eyF=1MECWhDRXjQyyLRre5g4GmpDgUESq8jXn2TFSkVhfhAs2KRfeMWv2htJYyLLK3GbRJKgKtDZFMMSsB2YbQID-ZoCltc2Peav7S5IZKYwPD47Kp7ZfZpiqr5cjNXINzEFTHIJrrwT26EEMZF=KGXWEBO9GGRTiW37rE7RsFydgVy8lWEaRjVD-V-py9ZU=2q8_BmLSE4=M733dIfXLcndttYO2rPiWECeKHkaoPHdh2ftMmhlBU27JYlPVYDhEiEZaJx7YJmAm4ms_4m=hiHhksCwZ_p4aBpc2mzmdHWgB5WTvP2STTu6egjNnFhImhBf=GWSqqoQnmHAb6IXkMuBMg3nJ3p1qK7xMku67Sw6XZEfUlbHFaD6gW2X7ZoGPUgHpOajZHmBks39wA3CTLFSVeMM3bLbOxNiOtLUFch2BI6FHu6b7n==6tWblpCpIZx5_e9tVaRV-NPlhSldJy9-KCDDcRGKCRt_YuyCnmvVzdseOVXs-MdSKlUTpan9XMFBeJrCnw-RQF8TbkaDY7ctCiIJCk3-2IQY_RQ76THQeVGb3LpbTWiAsPjbyemkNnAistExXpcr1-n=-NAMUiGJscu2sdTVb2tWtmk5r8OEn=AlD9RzqvG8nNr7f9STDRzkPVzODx2zj44vm3udbskccJyerOebZ5Gt1DkNxPpV6FW_JJ44eRDsXO6jHiUC-xFzxTDp_j9J5O5BeXjeH-XjO889zfKzNaTSCX=LB6BYePvyZYkV559Z_ckUuACeg9UO4ySu38HqiCuUn=ct2iOrcxpAaBajzYF=cSDmfhnlc9rhk_4zU1pem4WioHVMtAmeqR7eOYkMczmbV2meaWf-Qg6wu6IHYD=vZwsgoWHEGv3SU-T6F1Oh5YP-KMqyWUylcGzT58Kxq=X26LnzkCIxVJpLbWbDtl3YVam9vY4SCXEa2fYMZ4jiRNshbof51Lz9FBrt1qZfG4TgZy9EWLklxu8DNUrG2ZXmdFs9xSCtIZumoM1MivWjZg5zKCzUm26ZWs-5JVtbLlzWnpXDH-pTiBWXizJ8b8MCzabvJWB8vtwpELyCrq-CqCOO3QkMZ=-WwmQCkjBiZfo-UI-KnKvLkEC=NBFqLDg1hGJzNY-g4jHFMvBubAs4c7dFE_vAu=yDGl9e6HlEyzIxawgr6MnPlv_owdkq-Kr69SYYDOXUgPO5ScIgmuDOAiLO5N_klijbdxmAd8SzsQbN1Jy6UaF2U1wLypE5kCBMMeTl9szOO5BLpl7HrbP6ZiUhlhYGx9en5JXEu9BnHhSowAylX5=M9BeFuupxCpfjOn=2i3lRQE3_UiuJsLrHGi4WOjU6bvhe6md1lTqszYhXTzWuSlIu5_t5TE7qdAnSCU3d1thEJajGTHVKKkBZ-jIB1nFR8fnLyNTgJvwpB28opIVcR-lmBE_C4LAgyGM3EW3Bvyv94ksAF3hW6ERbA2xPDMgtkY6LRxbwu47596t3C2jKpshZo6D3sz4cee_yb4yw7QygMRSxGb6SOjqH_ZTcxYzu4TH1vgoET2msQqLfeyKZM19vQ5jq2N9lFmGxey27n4vTxiRLWaUuJucXMtgXrnapaGkmY6mMX3nSw7gWDNLuhzQVQmwo2CHP=Vj_4sWJ3IndNtZQ7866JanHILJJf6O4kt4ri9OTEmflG8f=ISMMoCcy9v-ivviL=A7k18c95fsEhREjud9pN-Ir-HS=-hi_-1SYcn5oHgXx3CLlwgv5_qa1EdUMeuRIheTZswAoWHdQR=GOEbfd82ECnlWXHa6k6DVvyEcpgDyb=qCImaoSHNoeRcIgZjmWIUJivLqPNeGevooPARIH_f_OpMeAFnGVt3jF3Q=rrZDQpZargFwY42fXQA2yTo_iLOwxHtpU4=JmsB6ZgFHrtX=Xvg=3PJeb1ZnPrAqPYAHTnZkTR4tVLuO7EpfaDnRoJkF4a4ed4Wa9MHjyoorM6AnhOe3riw4iWhB9WE4ds=ufV_LgYd1guhBkFKLHldfoPdAmLnEcKEDbA9CCzjDpLr6mfsyGe1sqa49GL5JYim78MvZVOW7pm8BbpDF=h4n_ZvgYnVRk3kkc5DjUN-oKDePlOQDCRbQdEhfKOofeeXTm4GuUDB7qSfJCC23CkWLy6bpurjDhZBgJu_5N5AMY=JuiTtYLxvhrEm8M1i2gAz6vO9jomxHmrAEY7jn_ZNWvvRZevNi=4Cd6zVRuDjC9jKaIbrt1ZmEhIOATt58mbLBfnYqnZmmeRwoFLftas7IQu6Pdc_8Rxu8hgMNOuAeUL5SJyvzBryJnMfb3VqLqbOUNSIGCRwflyZHrL-9B8x3EOozQOqUsGmsXLv_5tPzUszcPeUD15HAvleSoM114QJlIex9kZAbqYvISmOL=g=rVvu8U-J5DhVMFK1aHSquvDNtta-jBG=F-ZLUlqI2QEJxV98=gggWw9_u1XxEmn=B12KaCWAaenJB18MbVt3g72PlOp-QRigA7sXS4Cq1oefTshiDOq4TyJr6J97FUGfFJnFWxmQ8UHli369cDhCdusdq84WGq-saTtmydKWvdoICdcVp2POCQLEYbf1hkIJGEfxdS9hheYixN9DS7YD9gRMzE-1439eKAZlDJzZv4WV3o7nJbGyZVHD9cFKLUSLtZV5fm8zTa5rJfk44SYZMHmVEfO7vxRXg-xVOOvVXJMCV5-S8gisoUR4=rvGfmaJu-RewJzdKauVk1P3pWvMZkQ1i6OX4g4kdgtUOUzNH1sI4aRNxVpWgiklgdDlPW1Z43D5nV36g5pPHiIEyfOJnLl9Rr5tC7abyxtDjcsJvJEWPWAIrcdbdQqw_7c3PWd_Tth65=YXE7=P424fKA__AA6BRNIgI65C7hAUNNydgzxfpmRQBa6DzmqosfCYOug_5p_i_Olj9IWQgAJycg4M36ckGY6bikxuQ4HAOKxRLwot5-VWZYizJM6HjGzXF3FR25-6nYkA58fX6tVqklJybZkSE1Umwo22oHm7z2Ga1Nr1taR76-gyVzDkaMK-7xhxttqsVsn=p=S7zi7nPJFGzQVP36mZ7hkEuUNcTJj=Btx8=AupYVgGFhnT5hlBVWx1d35RsGWoE-vHxqIlSXzzqybQKejaCa4QdeZp5VIp=MzJC7RHd6Cx7tJYa2Ot3FAAmlcIm5buJltGIhQFI56O7Zu87Ua2BF3Qq2tVuzTboIHpb-tUz1_DiP5PtZqJ=fdkSYfVXa6air_T5_cB6Y-4fPfurF=Pl3h7uepxqVDF=6GpTXMZ2eOxT8wBx5CX7oR48Slps3=hdkaXFRXqJ7Y8tyxrAXRWd8rECfIQWKaFnUTp8e7qbRiOWh7zzZiz=tcUg8Rw9IHBZhB7NU3uvI4UhR1wlh5zQY2lmCqSnNCPSJ9omIKGDaoTPmTwE2xzr2Zxx8Kw-EVbRh_G-ITGXRwi6F6Srx5Lyu3yADd5cCRiPlvrelkZ',
        'Connection': 'keep-alive',
        'Referer': 'https://www.lkqonline.com/engine-assembly',
        'Cookie': 'CCbdcy63=A2j_zGGVAQAAPZJIXJhyQhdFCgcYlVjqQ6_5WYt78FfLsn0817gHIUmZEgxSAc_0R1CcuIhSwH9eCOfvosJeCA|1|1|56f227bbb94ee951081f539dbb88f28945c7a184',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin'
    }

    unique_product_ids = set()  # Set to track unique product IDs
    total_products = 0
    start_time = datetime.now()

    while True:
        url = f"{api_url}&skip={skip}&take={take}"
        response = fetch_with_retries(url, headers)

        if response is None:
            print("Failed to fetch data after retries")
            break

        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break

        if 'set-cookie' in response.headers:
            cookies = response.headers['set-cookie']
            headers['Cookie'] = cookies

        data = response.json()
        products = data.get("data", [])  # Adjust the key based on actual API response

        if not products or skip >= 12:
            break

        # Save products to the database
        save_products(conn, job_id, products)
        total_products += len(products)

        skip += take

    # Update job stats
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    update_job(conn, job_id, "completed", total_products, end_time, execution_time)

    print(f"Total unique products fetched: {total_products}")

# Main function to start the scraper
def start_scraper(api_url, scraper_name):
    conn = connect_to_db()
    if not conn:
        return

    # Create a job entry
    job_id = create_job(conn, scraper_name)
    if not job_id:
        conn.close()
        return

    # Start the scraper
    fetch_all_products(api_url, job_id=job_id, conn=conn)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    API_URL = "https://www.lkqonline.com/api/catalog/0/product?catalogId=0&category=Engine%20Compartment%7CEngine%20Assembly&sort=closestFirst&latitude=38.79995&longitude=-77.54425"
    SCRAPER_NAME = "lkq_scraper"

    # Start the scraper
    start_scraper(API_URL, SCRAPER_NAME)
