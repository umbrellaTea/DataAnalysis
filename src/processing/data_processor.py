import pandas as pd
import numpy as np

def clean_data(df):
    """清理数据框中的数据
    
    Args:
        df (DataFrame): 原始数据框
        
    Returns:
        DataFrame: 清理后的数据框
    """
    # 删除重复的电影条目
    df = df.drop_duplicates()
    
    # 将年份字符串转换为整数类型
    df['year'] = df['year'].astype(int)
    
    # 将评分和投票数转换为相应的数值类型
    df['rating'] = df['rating'].astype(float)
    df['votes'] = df['votes'].astype(int)
    
    return df

def add_features(df):
    """添加新的特征
    
    Args:
        df (DataFrame): 原始数据框
        
    Returns:
        DataFrame: 添加新特征后的数据框
    """
    # 添加年代特征（如1990年代、2000年代等）
    df['decade'] = (df['year'] // 10) * 10
    
    # 将电影类型字符串拆分为列表，便于后续分析
    df['genres'] = df['genre'].str.split(' ')
    
    return df 