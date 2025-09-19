import requests
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup
import threading
import time

headers = {'User-Agent': 'Mozilla/5.0 ...'}
MAX_THREADS = 10
lock = threading.Lock()

def extract_movie_details(movie_link):
    try:
        time.sleep(random.uniform(0, 0.2))
        res = requests.get(movie_link, headers=headers, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser')

        title = soup.find('h1').get_text(strip=True) if soup.find('h1') else None
        date = soup.find('a', title='See more release dates').get_text(strip=True) if soup.find('a', title='See more release dates') else None
        rating = soup.find('span', itemprop='ratingValue').get_text() if soup.find('span', itemprop='ratingValue') else None
        plot_text = soup.find('div', class_='summary_text').get_text(strip=True) if soup.find('div', class_='summary_text') else None

        if all([title, date, rating, plot_text]):
            print(title, date, rating, plot_text)
            with lock:
                with open('movies.csv', 'a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow([title, date, rating, plot_text])
    except Exception as e:
        print(f"Erro ao acessar {movie_link}: {e}")

def extract_movies(soup):
    movies = soup.select('table[data-caller-name="chart-moviemeter"] tbody tr')
    links = ['https://imdb.com' + tr.find('a')['href'] for tr in movies]
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(extract_movie_details, links)

def main():
    start = time.time()
    url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    soup = BeautifulSoup(requests.get(url, headers=headers).content, 'html.parser')
    extract_movies(soup)
    print('Total time taken:', time.time() - start)

if __name__ == '__main__':
    main()
