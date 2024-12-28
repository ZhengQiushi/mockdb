import sys
import os
import unittest
from collections import deque

# 确保能够导入核心模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from core.analyze.graph import Graph
from core.analyze.clump import Clump

class TestGraph(unittest.TestCase):

    def setUp(self):
        self.graph = Graph(weight=1, theta=1, top_hot_threshold=5)

    def test_add_transaction(self):
        # 测试添加事务并检查点权和边权
        self.graph.add_transaction([1, 2, 3])
        self.assertEqual(self.graph.vertices[1].weight, 1)
        self.assertEqual(self.graph.vertices[2].weight, 1)
        self.assertEqual(self.graph.vertices[3].weight, 1)
        self.assertIn(frozenset({1, 2}), self.graph.edges)
        self.assertEqual(self.graph.edges[frozenset({1, 2})].weight, 1)
        self.assertIn(frozenset({1, 3}), self.graph.edges)
        self.assertEqual(self.graph.edges[frozenset({1, 3})].weight, 1)
        self.assertIn(frozenset({2, 3}), self.graph.edges)
        self.assertEqual(self.graph.edges[frozenset({2, 3})].weight, 1)

    def test_get_hot_region(self):
        # 添加一些事务以构建图
        self.graph.add_transaction([1, 2, 3], weight=2)
        self.graph.add_transaction([2, 3, 4], weight=2)
        self.graph.add_transaction([3, 4, 5], weight=2)
        self.graph.add_transaction([6, 7, 8], weight=1)
        
        # 获取热点闭包
        hot_clumps = self.graph.get_hot_region(edge_thresh=0)
        
        # 检查结果
        self.assertEqual(len(hot_clumps), 2)
        
        # 第一个闭包应包含1,2,3,4,5
        clump1 = [clump for clump in hot_clumps if 1 in clump.region_ids][0]
        self.assertEqual(clump1.region_ids, set([1, 2, 3, 4, 5]))
        self.assertEqual(clump1.hot, 2 * 9)  # 假设每个region的点权是2，总共5个
        
        # 第二个闭包应包含6,7,8
        clump2 = [clump for clump in hot_clumps if 6 in clump.region_ids][0]
        self.assertEqual(clump2.region_ids, set([6, 7, 8]))
        self.assertEqual(clump2.hot, 3)  # 假设每个region的点权是1，总共3个

    def test_empty_transaction(self):
        # 测试空事务
        self.graph.add_transaction([])
        self.assertEqual(len(self.graph.vertices), 0)
        self.assertEqual(len(self.graph.edges), 0)

    def test_single_region_transaction(self):
        # 测试单个region的事务
        self.graph.add_transaction([1, 1])
        self.assertEqual(self.graph.vertices[1].weight, 2)
        self.assertEqual(len(self.graph.edges), 0)

    def test_edge_threshold(self):
        # 测试边权阈值
        self.graph.add_transaction([1, 2], weight=10)
        self.graph.add_transaction([2, 3], weight=10)
        self.graph.add_transaction([3, 4], weight=5)

        hot_clumps = self.graph.get_hot_region(edge_thresh=8)
        self.assertEqual(len(hot_clumps), 2)
        # 1,2,3应在一个闭包，3和4由于边权低于阈值，应分别在不同的闭包
        clump1 = [clump for clump in hot_clumps if 1 in clump.region_ids][0]
        self.assertEqual(clump1.region_ids, set([1, 2, 3]))

        clump2 = [clump for clump in hot_clumps if 4 in clump.region_ids][0]
        self.assertEqual(clump2.region_ids, set([4]))

if __name__ == '__main__':
    unittest.main()