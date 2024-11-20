import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import font_manager


class DataChecker:
    def __init__(self, file_path='data/raw/douban_movies_large.csv'):
        """初始化数据检查器"""
        self.file_path = file_path
        self.df = None
        
        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # Mac OS的中文字体
        plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
        
        # 设置图表样式
        plt.style.use('seaborn')
        sns.set_palette("husl")
        
    def load_data(self):
        """加载数据文件"""
        try:
            self.df = pd.read_csv(self.file_path, encoding='utf-8')
            print(f"成功加载数据文件: {self.file_path}")
            print(f"数据总条数: {len(self.df)}")
            return True
        except Exception as e:
            print(f"加载数据文件失败: {e}")
            return False
            
    def basic_info(self):
        """显示基本信息"""
        if self.df is None:
            return
            
        print("\n=== 基本信息 ===")
        print(self.df.info())
        
        print("\n=== 数据预览 ===")
        print(self.df.head())
        
        print("\n=== 基本统计 ===")
        print(self.df.describe())
        
    def check_missing_values(self):
        """检查缺失值"""
        if self.df is None:
            return
            
        print("\n=== 缺失值检查 ===")
        missing = self.df.isnull().sum()
        print(missing[missing > 0])
        
    def check_duplicates(self):
        """检查重复值"""
        if self.df is None:
            return
            
        print("\n=== 重复值检查 ===")
        duplicates = self.df.duplicated().sum()
        print(f"重复条目数: {duplicates}")
        
    def check_data_distribution(self):
        """检查数据分布"""
        if self.df is None:
            return
            
        print("\n=== 数据分布检查 ===")
        
        # 年份分布
        print("\n年份分布:")
        print(self.df['year'].value_counts().sort_index().head())
        
        # 评分分布
        print("\n评分分布:")
        print(self.df['rating'].value_counts().sort_index().head())
        
        # 国家/地区分布
        print("\n国家/地区分布（前5名）:")
        print(self.df['country'].value_counts().head())
        
    def plot_distributions(self):
        """绘制分布图"""
        if self.df is None:
            return
            
        # 创建图形
        plt.figure(figsize=(15, 12))
        
        # 评分分布
        plt.subplot(2, 2, 1)
        sns.histplot(data=self.df, x='rating', bins=20)
        plt.title('电影评分分布')
        plt.xlabel('评分')
        plt.ylabel('电影数量')
        
        # 年份分布
        plt.subplot(2, 2, 2)
        sns.histplot(data=self.df, x='year', bins=30)
        plt.title('电影年份分布')
        plt.xlabel('年份')
        plt.ylabel('电影数量')
        
        # 评分箱线图
        plt.subplot(2, 2, 3)
        sns.boxplot(y=self.df['rating'])
        plt.title('评分箱线图')
        plt.ylabel('评分')
        
        # 国家/地区分布（前10名）
        plt.subplot(2, 2, 4)
        country_counts = self.df['country'].value_counts().head(10)
        sns.barplot(x=country_counts.values, y=country_counts.index)
        plt.title('国家/地区分布（前10名）')
        plt.xlabel('电影数量')
        
        # 调整布局并保存
        plt.tight_layout()
        plt.savefig('results/data_distribution.png', dpi=300, bbox_inches='tight')
        print("\n分布图已保存至 results/data_distribution.png")
        
        # 关闭图形，释放内存
        plt.close()
        
        # 绘制额外的分析图
        self.plot_additional_analysis()
        
    def plot_additional_analysis(self):
        """绘制额外的分析图"""
        # 评分与年份的关系
        plt.figure(figsize=(12, 6))
        sns.scatterplot(data=self.df, x='year', y='rating', alpha=0.5)
        plt.title('电影评分与年份关系')
        plt.xlabel('年份')
        plt.ylabel('评分')
        plt.savefig('results/rating_year_relationship.png', dpi=300, bbox_inches='tight')
        plt.close()
        
    def run_all_checks(self):
        """运行所有检查"""
        if not self.load_data():
            return
            
        self.basic_info()
        self.check_missing_values()
        self.check_duplicates()
        self.check_data_distribution()
        self.plot_distributions()
        
if __name__ == "__main__":
    # 创建结果目录
    os.makedirs('results', exist_ok=True)
    
    # 运行检查
    checker = DataChecker()
    checker.run_all_checks() 
    