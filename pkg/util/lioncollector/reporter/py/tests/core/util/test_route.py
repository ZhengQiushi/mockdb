import unittest
import json
from core.util.route import Route

class TestRoute(unittest.TestCase):

    def setUp(self):
        self.route = Route()

    # def test_update_region_from_pd(self):
    #     # 调用 update_region_from_pd
    #     self.route.update_region_from_pd("http://10.77.70.205:10080/tables/benchbase/usertable/regions")
    #     self.route.save("history/router.pkl.148")
    
    def test_route_analyze(self):
        """
        分析 virtual_region_id 的分布情况，统计每十个 virtual_region_id 范围内主节点 store_id 的出现次数。
        如果某个范围内有多个 store_id，则打印对应的 real region_id。
        :return: 一个字典，key 是 virtual_region_id 的范围，value 是一个字典，表示 store_id 及其出现次数。
        """
        self.route.update_region_from_pd("http://10.77.70.205:10080/tables/benchbase/usertable/regions")
        # 初始化结果字典
        result = {}

        # 获取所有 virtual_region_id
        virtual_region_ids = sorted(self.route.virtual_region_id_map.keys())

        # 每十个 virtual_region_id 进行统计
        for i in range(0, len(virtual_region_ids), 10):
            start = i
            end = min(i + 10, len(virtual_region_ids))
            range_key = f"{start}-{end-1}"

            # 统计当前范围内的 store_id 出现次数
            store_id_count = {}
            real_region_ids = []  # 用于存储 real region_id
            for virtual_id in virtual_region_ids[start:end]:
                actual_id = self.route.virtual_region_id_map[virtual_id]
                store_id = self.route.region_primary_store_id[actual_id]
                if store_id in store_id_count:
                    store_id_count[store_id] += 1
                else:
                    store_id_count[store_id] = 1
                real_region_ids.append(actual_id)  # 记录 real region_id

            # 将统计结果添加到最终结果中
            result[range_key] = store_id_count

            # 如果 store_id_count 的 len > 1，则打印对应的 real region_id
            if len(store_id_count) > 1:
                print(f"Range {range_key} has multiple store_ids: {store_id_count}")
                print(f"Corresponding real region_ids: {real_region_ids}")

        # 格式化打印结果
        print("\nFormatted Analysis Result:")
        for range_key, store_id_count in result.items():
            print(f"{range_key}:")
            for store_id, count in store_id_count.items():
                print(f"  Store ID {store_id}: {count} regions")
            print()  # 空行分隔

        return result
    # def test_update_region_from_pd(self):
    #     # 调用 update_region_from_pd
    #     self.route.update_region_from_pd("http://10.77.70.205:10080/tables/benchbase/usertable/regions")
    #     self.route.save("history/router.pkl.205")

    # def test_update_region_from_pd_mock(self):
    #     mock_response = {
    #         "name": "usertable",
    #         "id": 112,
    #         "record_regions": [
    #             {
    #                 "region_id": 2005,
    #                 "leader": {"id": 2007, "store_id": 7},
    #                 "peers": [
    #                     {"id": 2006, "store_id": 2},
    #                     {"id": 2007, "store_id": 7},
    #                     {"id": 2008, "store_id": 1}
    #                 ],
    #                 "region_epoch": {"conf_ver": 5, "version": 71}
    #             },
    #             {
    #                 "region_id": 2009,
    #                 "leader": {"id": 2011, "store_id": 7},
    #                 "peers": [
    #                     {"id": 2010, "store_id": 2},
    #                     {"id": 2011, "store_id": 7},
    #                     {"id": 2012, "store_id": 1}
    #                 ],
    #                 "region_epoch": {"conf_ver": 5, "version": 67}
    #             }
    #         ],
    #         "indices": []
    #     }

    #     # 调用 update_region_from_pd
    #     self.route.update_region(mock_response)

    #     # 检查 store_ids
    #     self.assertEqual(self.route.get_all_store_ids(), {1, 2, 7})

    #     # 检查 region_primary_store_id
    #     self.assertEqual(self.route.get_region_primary_store_id(0), 7)
    #     self.assertEqual(self.route.get_region_primary_store_id(1), 7)

    #     # 检查 region_secondary_store_id
    #     self.assertEqual(self.route.get_region_secondary_store_id(0), [2, 1])
    #     self.assertEqual(self.route.get_region_secondary_store_id(1), [2, 1])

    # def test_get_region_primary_store_id_not_found(self):
    #     # 测试获取不存在的 region_id 的主节点
    #     self.assertIsNone(self.route.get_region_primary_store_id(9999))

    # def test_get_region_secondary_store_id_not_found(self):
    #     # 测试获取不存在的 region_id 的从节点
    #     self.assertEqual(self.route.get_region_secondary_store_id(9999), [])

if __name__ == '__main__':
    unittest.main()