import json
import os
from datetime import datetime

def check_progress():
    """检查爬虫进度"""
    checkpoint_file = 'data/raw/crawler_checkpoint.json'
    
    if not os.path.exists(checkpoint_file):
        print("未找到断点文件，爬虫可能未开始或已完成")
        return
    
    try:
        with open(checkpoint_file, 'r') as f:
            data = json.load(f)
            
        # 获取文件修改时间
        mod_time = os.path.getmtime(checkpoint_file)
        last_update = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
            
        print(f"\n=== 爬虫进度检查 ===")
        print(f"最后更新时间: {last_update}")
        print(f"已爬取电影数量: {len(data['all_movies'])}")
        print(f"当前URL索引: {data['current_url_index'] + 1}/{4}")  # 4是base_urls的长度
        print(f"当前URL已爬取数量: {data['movies_count']}")
        
        # 显示最近爬取的5部电影
        print("\n最近爬取的5部电影:")
        for movie in data['all_movies'][-5:]:
            print(f"- {movie['title']} ({movie['year']}) - 评分: {movie['rating']}")
            
    except Exception as e:
        print(f"读取断点文件时出错: {e}")

if __name__ == "__main__":
    check_progress() 