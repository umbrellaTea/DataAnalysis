import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import urllib3

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class FinalMovieCrawler:
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
        
        # 加载9000条数据
        self.total_movies = pd.read_csv('data/raw/douban_movies_9000.csv').to_dict('records')
        print(f"Loaded {len(self.total_movies)} existing records")
        
        # 记录已爬取的URL，避免重复
        self.crawled_urls = set()
        for movie in self.total_movies:
            if 'url' in movie:
                self.crawled_urls.add(movie['url'])
    
    def get_movie_detail(self, url):
        """获取电影详细信息"""
        if url in self.crawled_urls:
            return None
        
        max_retries = 3
        for retry in range(max_retries):
            try:
                self.headers['User-Agent'] = random.choice(self.user_agents)
                # 禁用SSL验证，添加超时设置
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    verify=False,  # 禁用SSL验证
                    timeout=10     # 设置超时
                )
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 检查页面是否存在且有评分
                    rating_element = soup.find('strong', class_='ll rating_num')
                    if not rating_element or not rating_element.text.strip():
                        return None  # 跳过没有评分的电影
                        
                    info = soup.find('div', id='info').text.strip()
                    year = soup.find('span', class_='year')
                    year = year.text.strip('()') if year else ''
                    
                    try:
                        director = info.split('导演: ')[1].split('\n')[0].strip()
                    except:
                        director = ''
                        
                    try:
                        genres = info.split('类型:')[1].split('\n')[0].strip()
                    except:
                        genres = ''
                        
                    try:
                        country = info.split('制片国家/地区:')[1].split('\n')[0].strip()
                    except:
                        country = ''
                        
                    try:
                        language = info.split('语言:')[1].split('\n')[0].strip()
                    except:
                        language = ''
                        
                    try:
                        rating = float(rating_element.text.strip())
                    except:
                        return None  # 如果评分无法转换为数字，跳过
                        
                    try:
                        votes = soup.find('span', property='v:votes')
                        votes = int(votes.text) if votes else 0
                    except:
                        votes = 0
                        
                    title_element = soup.find('span', property='v:itemreviewed')
                    if not title_element:
                        return None
                        
                    self.crawled_urls.add(url)
                    return {
                        'title': title_element.text,
                        'year': int(year) if year.isdigit() else 0,
                        'director': director,
                        'genres': genres,
                        'country': country,
                        'language': language,
                        'rating': rating,
                        'votes': votes,
                        'url': url
                    }
                elif response.status_code == 403:
                    print(f"Access denied for {url}, sleeping...")
                    time.sleep(5)
                else:
                    print(f"Status {response.status_code} for {url}, retry {retry + 1}")
                    time.sleep(2)
                
            except (requests.exceptions.SSLError, 
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                print(f"Network error for {url}: {str(e)}, retry {retry + 1}/{max_retries}")
                time.sleep(2 * (retry + 1))
            except Exception as e:
                print(f"Error for {url}: {str(e)}")
                time.sleep(2)
        return None

    def crawl_final_batch(self):
        """爬取最后800条数据"""
        target = 10000
        search_url = "https://movie.douban.com/j/new_search_subjects"
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            while len(self.total_movies) < target:
                try:
                    # 使用搜索API获取电影列表
                    params = {
                        'sort': 'rating',
                        'range': '0,10',
                        'tags': '电影',
                        'start': len(self.total_movies),
                        'limit': 50
                    }
                    
                    self.headers['User-Agent'] = random.choice(self.user_agents)
                    response = requests.get(search_url, headers=self.headers, params=params)
                    
                    if response.status_code == 200:
                        movies = response.json().get('data', [])
                        if not movies:
                            print("No more movies returned")
                            break
                        
                        futures = []
                        for movie in movies:
                            if len(self.total_movies) >= target:
                                break
                            futures.append(
                                executor.submit(self.get_movie_detail, movie['url'])
                            )
                        
                        for future in concurrent.futures.as_completed(futures):
                            movie_detail = future.result()
                            if movie_detail and len(self.total_movies) < target:
                                self.total_movies.append(movie_detail)
                                current_count = len(self.total_movies)
                                print(f"Crawled: {movie_detail['title']} ({current_count}/{target})")
                
                    time.sleep(random.uniform(1, 2))
                    
                except Exception as e:
                    print(f"Error occurred: {str(e)}")
                    time.sleep(5)
                    continue
            
            # 只在最后保存一次
            df = pd.DataFrame(self.total_movies)
            df.to_csv('data/raw/douban_movies_final.csv', index=False, encoding='utf-8')
            print(f"Successfully crawled all {len(self.total_movies)} movies!")
            return df

if __name__ == "__main__":
    crawler = FinalMovieCrawler()
    movies_df = crawler.crawl_final_batch() 