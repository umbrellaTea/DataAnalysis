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
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Cookie': 'your_cookie_here'
        }
        self.search_url = "https://movie.douban.com/j/new_search_subjects"
        
        # 加载已爬取的数据
        try:
            self.total_movies = pd.read_csv('data/raw/douban_movies_9000.csv').to_dict('records')
            print(f"Loaded {len(self.total_movies)} existing records")
        except:
            self.total_movies = []
            print("Starting fresh crawl")
            
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
                print(f"Requesting movies with params: {params}")  # 打印请求参数
                response = requests.get(self.search_url, headers=self.headers, params=params)
                print(f"Response status: {response.status_code}")  # 打印响应状态码
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"Response data: {data}")  # 打印响应数据
                    return data.get('data', [])
                    
                print(f"Retry {retry + 1}/{max_retries}, status code: {response.status_code}")
                time.sleep(5 * (retry + 1))
            except Exception as e:
                print(f"Error fetching movies: {e}, retry {retry + 1}/{max_retries}")
                time.sleep(5 * (retry + 1))
        return []

    def crawl_movies(self, total=10000):
        """爬取指定数量的电影"""
        remaining = total - len(self.total_movies)
        if remaining <= 0:
            print("Already have enough records")
            return pd.DataFrame(self.total_movies)
        
        start = len(self.total_movies)
        batch_size = 10  # 每批10条
        last_save_count = len(self.total_movies)
        
        print(f"Continuing crawl from position {start}, aiming for {remaining} more records")
        print("Using optimized crawling strategy...")
        
        with ThreadPoolExecutor(max_workers=3) as executor:  # 使用3个线程
            while len(self.total_movies) < total:
                try:
                    # 获取单批次数据
                    movies = self.get_movies(start, batch_size)
                    if not movies:
                        print("No movies returned, sleeping for 30 seconds...")
                        time.sleep(30)
                        continue
                    
                    # 并行处理电影
                    futures = []
                    for movie in movies:
                        if len(self.total_movies) >= total:
                            break
                        futures.append(
                            executor.submit(self.get_movie_detail, movie['url'])
                        )
                    
                    # 收集结果
                    for future in futures:
                        movie_detail = future.result()
                        if movie_detail and len(self.total_movies) < total:
                            self.total_movies.append(movie_detail)
                            current_count = len(self.total_movies)
                            print(f"Crawled: {movie_detail['title']} ({current_count}/{total})")
                            
                            # 每50条保存一次
                            if current_count % 50 == 0:
                                df = pd.DataFrame(self.total_movies)
                                save_path = f'data/raw/douban_movies_{current_count}.csv'
                                df.to_csv(save_path, index=False, encoding='utf-8')
                                print(f"Saved progress to {save_path}")
                
                    start += batch_size
                    # 每批次后休息3-5秒
                    time.sleep(random.uniform(3, 5))
                    
                except Exception as e:
                    print(f"Error occurred: {str(e)}")
                    print("Sleeping for 30 seconds before retry...")
                    time.sleep(30)
        
        # 保存最终数据
        df = pd.DataFrame(self.total_movies)
        df.to_csv('data/raw/douban_movies_final.csv', index=False, encoding='utf-8')
        print(f"Successfully crawled {len(self.total_movies)} movies!")
        return df

if __name__ == "__main__":
    crawler = DoubanMovieCrawler()
    movies_df = crawler.crawl_movies(total=10000)