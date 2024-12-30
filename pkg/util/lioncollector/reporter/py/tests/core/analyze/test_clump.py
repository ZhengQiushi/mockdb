import os
import sys
import unittest
from collections import deque

# 确保能够导入核心模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from core.analyze.graph import Graph
from core.analyze.clump import Clump

class TestClump(unittest.TestCase):

    def setUp(self):
        # 构建图文件的路径
        graph_file = os.path.join('history', 'graph_1735439924.pkl')
        # 加载图对象
        self.graph = Graph.load(graph_file)
        # 设置边权阈值
        self.edge_thresh = 0  # 根据需要调整

    def test_get_hot_region(self):
        # 获取热点闭包
        hot_clumps = self.graph.get_hot_region(self.edge_thresh)
        print(len(hot_clumps)) ## 88
        print(hot_clumps) ## 88


if __name__ == '__main__':
    unittest.main()