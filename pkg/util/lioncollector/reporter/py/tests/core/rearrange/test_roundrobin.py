import os
import sys
import unittest

# 确保能够导入核心模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from core.rearrange.subplan import SubPlan
from core.analyze.clump import Clump
from core.util.route import Route
from core.rearrange.adaptor import Adaptor, OpPlan
from core.rearrange.planner import Planner
from core.analyze.graph import Graph


class TestRoundRobin(unittest.TestCase):

    def setUp(self):
        # # 构建图文件的路径
        # graph_file = os.path.join('history', 'graph_1735442958.pkl.uniform')
        # # 加载图对象
        # self.graph = Graph.load(graph_file)
        # # 设置边权阈值
        # self.edge_thresh = 0  # 根据需要调整

        # # 获取热点闭包
        # hot_clumps = self.graph.get_hot_region(self.edge_thresh)
        # print(len(hot_clumps)) ## 100
        # # print(hot_clumps) ## 

        # 构建文件的路径
        self.route = Route()
        self.route.update_region_from_pd("http://10.77.70.205:10080/tables/benchbase/usertable/regions")

        # planner = Planner(self.route, self.graph, weight=10, threshold=0.1, batch_size=5)
        # self.subplans = planner.generate_subplan(hot_clumps)

        # 初始化Adaptor对象
        self.pd_api_url = "http://10.77.70.117:2379"
        self.adaptor = Adaptor(self.pd_api_url, self.route, False)



    # def test_generate_op_plan(self):
    #     # 生成operator计划
    #     op_plans = self.adaptor.generate_op_plans(self.subplans)

    #     # 验证生成的operator计划
    #     expected_op_plans = [
    #         OpPlan(0, 0),  # 空的OpPlan
    #         OpPlan(1, 2, [{"operator": "transfer_leader", "region_id": 2, "to_store": 3}]),
    #         OpPlan(2, 3, [
    #             {"operator": "transfer_peer", "region_id": 3, "from_store": 1, "to_store": 4},
    #             {"operator": "transfer_leader", "region_id": 3, "to_store": 4}
    #         ])
    #     ]
    #     self.assertEqual(len(op_plans), len(expected_op_plans))
    #     for i in range(len(op_plans)):
    #         self.assertEqual(op_plans[i].subplan_index, expected_op_plans[i].subplan_index)
    #         self.assertEqual(op_plans[i].op_str, expected_op_plans[i].op_str)

    def test_do_operator_plan(self):
        # # 生成operator计划
        # op_plans = self.adaptor.generate_op_plans(self.subplans)

        # # 执行operator计划
        # self.adaptor.do_operator_plan(op_plans)
        self.adaptor.set_round_robin(False)




if __name__ == '__main__':
    unittest.main()