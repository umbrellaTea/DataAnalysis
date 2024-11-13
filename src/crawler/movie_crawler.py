import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import json
from concurrent.futures import ThreadPoolExecutor

class DoubanMovieCrawler:
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Firefox/89.0'
        ]
        self.headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.search_url = "https://movie.douban.com/j/new_search_subjects"
        self.total_movies = []
        
    def get_movie_detail(self, url, max_retries=3):
        """获取电影详细信息，添加重试机制"""
        for retry in range(max_retries):
            try:
                self.headers['User-Agent'] = random.choice(self.user_agents)
                response = requests.get(url, headers=self.headers)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 获取基本信息
                    info = soup.find('div', id='info').text.strip()
                    
                    # 解析年份
                    year = soup.find('span', class_='year').text.strip('()') if soup.find('span', class_='year') else ''
                    
                    # 解析导演
                    director = info.split('导演: ')[1].split('\n')[0].strip() if '导演: ' in info else ''
                    
                    # 解析类型
                    genres = info.split('类型:')[1].split('\n')[0].strip() if '类型:' in info else ''
                    
                    # 解析制片国家/地区
                    country = info.split('制片国家/地区:')[1].split('\n')[0].strip() if '制片国家/地区:' in info else ''
                    
                    # 解析语言
                    language = info.split('语言:')[1].split('\n')[0].strip() if '语言:' in info else ''
                    
                    # 获取评分和评价人数
                    rating = soup.find('strong', class_='ll rating_num').text.strip() if soup.find('strong', class_='ll rating_num') else '0'
                    votes = soup.find('span', property='v:votes').text if soup.find('span', property='v:votes') else '0'
                    
                    return {
                        'title': soup.find('span', property='v:itemreviewed').text if soup.find('span', property='v:itemreviewed') else '',
                        'year': int(year) if year.isdigit() else 0,
                        'director': director,
                        'genres': genres,
                        'country': country,
                        'language': language,
                        'rating': float(rating),
                        'votes': int(votes)
                    }
            except Exception as e:
                print(f"Error getting movie detail: {e}, retry {retry + 1}/{max_retries}")
                time.sleep(5 * (retry + 1))
        return None
    
    def get_movies(self, start, count=20, max_retries=3):
        """获取电影列表，添加重试机制"""
        for retry in range(max_retries):
            try:
                self.headers['User-Agent'] = random.choice(self.user_agents)
                params = {
                    'sort': 'rating',
                    'range': '0,10',
                    'tags': '电影',
                    'start': start,
                    'limit': count
                }
                response = requests.get(self.search_url, headers=self.headers, params=params)
                if response.status_code == 200:
                    return response.json().get('data', [])
                print(f"Retry {retry + 1}/{max_retries}, status code: {response.status_code}")
                time.sleep(5 * (retry + 1))  # 增加重试等待时间
            except Exception as e:
                print(f"Error fetching movies: {e}, retry {retry + 1}/{max_retries}")
                time.sleep(5 * (retry + 1))
        return []

    def crawl_movies(self, total=10000):
        """爬取指定数量的电影"""
        start = 0
        batch_size = 20
        consecutive_failures = 0
        last_save_count = 0  # 记录上次保存时的数据量
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            while len(self.total_movies) < total:
                movies = self.get_movies(start, batch_size)
                if not movies:
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        print("Too many consecutive failures, sleeping for 2 minutes...")
                        time.sleep(120)
                        consecutive_failures = 0
                    continue
                
                consecutive_failures = 0
                
                futures = []
                for movie in movies:
                    if len(self.total_movies) >= total:
                        break
                    futures.append(
                        executor.submit(self.get_movie_detail, movie['url'])
                    )
                
                for future in futures:
                    movie_detail = future.result()
                    if movie_detail and len(self.total_movies) < total:
                        self.total_movies.append(movie_detail)
                        current_count = len(self.total_movies)
                        print(f"Crawled: {movie_detail['title']} ({current_count}/{total})")
                        
                        # 每1000条数据保存一次
                        if current_count // 1000 > last_save_count // 1000:
                            df = pd.DataFrame(self.total_movies)
                            save_path = f'data/raw/douban_movies_{current_count}.csv'
                            df.to_csv(save_path, index=False, encoding='utf-8')
                            print(f"\nSaved {current_count} movies to {save_path}")
                            last_save_count = current_count
                
                start += batch_size
                time.sleep(random.uniform(1, 2))
        
        # 保存最终数据
        df = pd.DataFrame(self.total_movies)
        df.to_csv('data/raw/douban_movies_final.csv', index=False, encoding='utf-8')
        print(f"Successfully crawled {len(self.total_movies)} movies!")
        return df

if __name__ == "__main__":
    crawler = DoubanMovieCrawler()
    movies_df = crawler.crawl_movies(total=10000)