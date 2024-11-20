import pandas as pd

# 读取文件
df = pd.read_csv('data/raw/douban_movies_final.csv')

# 删除 url 列
df = df.drop('url', axis=1)

# 保存回文件
df.to_csv('data/raw/douban_movies_final.csv', index=False, encoding='utf-8')

print("Successfully removed url column!")