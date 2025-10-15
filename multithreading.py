import requests
import csv
import random
import concurrent.futures
import threading
import time
from bs4 import BeautifulSoup

headers = {'User-Agent': 'Mozilla/5.0'}
MAX_THREADS = 10
lock = threading.Lock()


def extract_movie_details(movie_link):
    """Extrai título, data, nota e resumo de um filme e salva no CSV."""
    try:
        time.sleep(random.uniform(0, 0.2))  # pequeno delay para evitar bloqueio
        res = requests.get(movie_link, headers=headers, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser')

        title = soup.find('h1').get_text(strip=True) if soup.find('h1') else None
        date = soup.find('a', title='See more release dates').get_text(strip=True) if soup.find('a', title='See more release dates') else None
        rating = soup.find('span', itemprop='ratingValue').get_text(strip=True) if soup.find('span', itemprop='ratingValue') else None
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
    """Extrai links dos filmes da tabela principal do IMDB."""
    movies = soup.select('table[data-caller-name="chart-moviemeter"] tbody tr')
    links = ['https://imdb.com' + tr.find('a')['href'] for tr in movies if tr.find('a')]
    return links


def main():
    """Cria o CSV e executa o scraping usando multithreading."""
    start = time.time()

    url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    links = extract_movies(soup)

    # cria o CSV com cabeçalho antes das threads começarem
    with open('movies.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Title', 'Release Date', 'Rating', 'Plot Summary'])

    # executa as threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(extract_movie_details, links)

    print('Total time taken:', time.time() - start)


if __name__ == '__main__':
    main()
