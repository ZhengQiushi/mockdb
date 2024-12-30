import os
import sys
import unittest
from collections import deque

# 确保能够导入核心模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from core.analyze.graph import Graph
from core.analyze.clump import Clump
from core.util.route import Route
from core.rearrange.planner import Planner
from core.rearrange.subplan import SubPlan

class TestPlanner(unittest.TestCase):
    def setUp(self):
        # 初始化路由信息
        self.route_mock = Route()
        self.route_data = {
            "name": "usertable",
            "id": 125,
            "record_regions": [
                {
                    "region_id": 6023,
                    "leader": {"id": 8462, "store_id": 3},
                    "peers": [
                        {"id": 6024, "store_id": 2},
                        {"id": 6025, "store_id": 9},
                        {"id": 8462, "store_id": 3},
                        {"id": 10915, "store_id": 8, "role": 1}
                    ],
                    "region_epoch": {"conf_ver": 78, "version": 390}
                },
                {
                    "region_id": 6035,
                    "leader": {"id": 6037, "store_id": 9},
                    "peers": [
                        {"id": 6037, "store_id": 9},
                        {"id": 9698, "store_id": 8},
                        {"id": 10795, "store_id": 3}
                    ],
                    "region_epoch": {"conf_ver": 83, "version": 329}
                },
                {
                    "region_id": 6047,
                    "leader": {"id": 6049, "store_id": 9},
                    "peers": [
                        {"id": 6049, "store_id": 9},
                        {"id": 6050, "store_id": 1},
                        {"id": 8470, "store_id": 3}
                    ],
                    "region_epoch": {"conf_ver": 77, "version": 327}
                },
                {
                    "region_id": 6051,
                    "leader": {"id": 10163, "store_id": 3},
                    "peers": [
                        {"id": 6054, "store_id": 1},
                        {"id": 10163, "store_id": 3},
                        {"id": 10645, "store_id": 2},
                        {"id": 10917, "store_id": 9, "role": 1}
                    ],
                    "region_epoch": {"conf_ver": 90, "version": 325}
                }
            ]
        }
        self.route_mock.update_region(self.route_data)

        # 初始化Planner
        self.planner_mock = Planner(self.route_mock, None, weight=10)

    # def test_evaluate_single_region_clump(self):
    #     """
    #     测试单个region的Clump的开销计算。
    #     """
    #     clump = Clump(region_ids={0}, hot=10)  # 虚拟region_id 0 对应实际region_id 6023
    #     costs = self.planner_mock.evaluate(clump, self.route_mock)

    #     # 预期结果：
    #     # - store_id 3: 主副本 (1 * 10) + 从副本 0 = -10
    #     # - store_id 2: 从副本 1 = -1
    #     # - store_id 9: 从副本 1 = -1
    #     # - store_id 8: 从副本 1 = -1
    #     # - 其他store_id: 0
    #     expected_costs = {
    #         1: 0,
    #         2: -1,
    #         3: -10,
    #         8: -1,
    #         9: -1
    #     }
    #     self.assertEqual(costs, expected_costs)

    # def test_evaluate_multiple_region_clump(self):
    #     """
    #     测试多个region的Clump的开销计算。
    #     """
    #     clump = Clump(region_ids={0, 1}, hot=20)  # 虚拟region_id 0 对应实际region_id 6023，虚拟region_id 1 对应实际region_id 6035
    #     costs = self.planner_mock.evaluate(clump, self.route_mock)

    #     # 预期结果：
    #     # - store_id 3: 主副本 1 (region_id 6023) + 从副本 1 (region_id 6035) = - (1 * 10 + 1) = -11
    #     # - store_id 2: 从副本 1 (region_id 6023) = -1
    #     # - store_id 9: 主副本 1 (region_id 6035) + 从副本 1 (region_id 6023) = - (1 * 10 + 1) = -11
    #     # - store_id 8: 从副本 1 (region_id 6023) + 从副本 1 (region_id 6035) = -2
    #     # - 其他store_id: 0
    #     expected_costs = {
    #         1: 0,
    #         2: -1,
    #         3: -11,
    #         8: -2,
    #         9: -11
    #     }
    #     self.assertEqual(costs, expected_costs)

    # def test_evaluate_no_region_clump(self):
    #     """
    #     测试没有region的Clump的开销计算。
    #     """
    #     clump = Clump(region_ids=set(), hot=0)
    #     costs = self.planner_mock.evaluate(clump, self.route_mock)

    #     # 预期结果：所有store_id的开销为0
    #     expected_costs = {
    #         1: 0,
    #         2: 0,
    #         3: 0,
    #         8: 0,
    #         9: 0
    #     }
    #     self.assertEqual(costs, expected_costs)

    # def test_skew_plan(self):
    #     # 构建图文件的路径
    #     graph_file = os.path.join('history', 'graph_1735439924.pkl')
    #     # 加载图对象
    #     self.graph = Graph.load(graph_file)
    #     # 设置边权阈值
    #     self.edge_thresh = 0  # 根据需要调整

    #     # 获取热点闭包
    #     hot_clumps = self.graph.get_hot_region(self.edge_thresh)
    #     print(len(hot_clumps)) ## 88
    #     print(hot_clumps) ## 88

    def test_uniform_plan(self):
        # 构建图文件的路径
        graph_file = os.path.join('history', 'graph_1735442958.pkl.uniform')
        # 加载图对象
        self.graph = Graph.load(graph_file)
        # 设置边权阈值
        self.edge_thresh = 0  # 根据需要调整

        # 获取热点闭包
        hot_clumps = self.graph.get_hot_region(self.edge_thresh)
        print(len(hot_clumps)) ## 100
        # print(hot_clumps) ## 

        # 构建文件的路径
        router_file = os.path.join('history', 'router.pkl')
        # 加载对象
        self.route = Route.load(router_file)

        planner = Planner(self.route, self.graph, weight=10, threshold=0.1, batch_size=5)
        subplans = planner.generate_subplan(hot_clumps)

        # # 输出生成的SubPlan列表
        # for subplan in subplans:
        #     print(subplan)
            
if __name__ == '__main__':
    unittest.main()