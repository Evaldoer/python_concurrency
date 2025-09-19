from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup

base_url = 'http://quotes.toscrape.com/page/'

def generate_urls():
    return [base_url + str(i) for i in range(1, 11)]

def scrape(url):
    try:
        res = requests.get(url, timeout=5)
        print(res.status_code, res.url)
        # Aqui você pode usar o BeautifulSoup para extrair os quotes, por exemplo:
        # soup = BeautifulSoup(res.text, 'html.parser')
        # quotes = [q.text for q in soup.select('.text')]
        # print(quotes)
    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")

if __name__ == "__main__":
    urls = generate_urls()
    with Pool(10) as p:
        p.map(scrape, urls)
