import os
import sys
import unittest

# 确保能够导入核心模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from core.rearrange.subplan import SubPlan
from core.analyze.clump import Clump
from core.util.route import Route
from core.rearrange.adaptor import Adaptor, OpPlan

class TestAdaptor(unittest.TestCase):

    def setUp(self):
        # 初始化Route对象
        self.route = Route()
        # 模拟路由数据
        self.route.virtual_region_id_map = {0: 1, 1: 2, 2: 3}
        self.route.region_primary_store_id = {1: 1, 2: 2, 3: 3}
        self.route.region_secondary_store_id = {1: [2, 3], 2: [1, 3], 3: [1, 2]}
        self.route.store_ids = {1, 2, 3, 4}

        # 初始化Adaptor对象
        self.pd_api_url = "http://10.77.110.148:2379"
        self.adaptor = Adaptor(self.pd_api_url, self.route, True)

        # 模拟SubPlan数据
        self.subplans = [
            SubPlan(Clump(region_ids=[0], hot=1.0), [1, 2], 1),  # 目标store已经是主节点
            SubPlan(Clump(region_ids=[1], hot=1.0), [2, 3], 3),  # 目标store是从节点
            SubPlan(Clump(region_ids=[2], hot=1.0), [1, 2], 4)   # 目标store不存在副本
        ]

    # def test_generate_op_plan(self):
    #     # 生成operator计划
    #     op_plans = self.adaptor.generate_op_plan(self.subplans)

    #     # 验证生成的operator计划
    #     expected_op_plans = [
    #         OpPlan(0),  # 空的OpPlan
    #         OpPlan(1, [{"operator": "transfer_leader", "region_id": 2, "target_store": 3}]),
    #         OpPlan(2, [
    #             {"operator": "transfer_peer", "region_id": 3, "from_store": 1, "to_store": 4},
    #             {"operator": "transfer_leader", "region_id": 3, "target_store": 4}
    #         ])
    #     ]
    #     self.assertEqual(len(op_plans), len(expected_op_plans))
    #     for i in range(len(op_plans)):
    #         self.assertEqual(op_plans[i].subplan_index, expected_op_plans[i].subplan_index)
    #         self.assertEqual(op_plans[i].op_str, expected_op_plans[i].op_str)

    # def test_do_operator_plan(self):
    #     # 生成operator计划
    #     op_plans = self.adaptor.generate_op_plan(self.subplans)

    #     # 执行operator计划
    #     self.adaptor.do_operator_plan(op_plans)


    def test_do_operator_plan_real(self):
        self.adaptor.mock = False
        # 生成operator计划
        op_plan = OpPlan(1)
        op_plan.add_op({
            "operator": "transfer_peer",
            "region_id": 15157,
            "from_store": 8,
            "to_store": 3
        })
        op_plans = []
        op_plans.append(op_plan)

        # 执行operator计划
        self.adaptor.do_operator_plan(op_plans)



if __name__ == '__main__':
    unittest.main()