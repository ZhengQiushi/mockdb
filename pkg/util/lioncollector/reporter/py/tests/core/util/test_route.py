import unittest
import json
from core.util.route import Route

class TestRoute(unittest.TestCase):

    def setUp(self):
        self.route = Route()
        self.route.update_region_from_pd("http://10.77.70.212:10080/tables/benchbase/usertable/regions")
        self.route.update_stores("http://10.77.70.250:12379/pd/api/v1/stores")

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

    def test_print_region_distribution(self):
        """
        打印当前所有 region 的主副本分布情况。
        表格形式展示每个节点上的主副本分布情况。
        """
        # 获取所有 store_id
        store_ids = sorted(self.route.get_all_store_ids())
        
        # 获取 virtual_region_id 和 real region_id 的映射关系
        virtual_region_id_map = self.route.virtual_region_id_map
        
        # 获取所有 region 的主副本和从副本信息
        region_primary_store_ids = self.route.region_primary_store_id
        region_secondary_store_ids = self.route.region_secondary_store_id
        
        # 打印表格标题
        print("\nRegion Replica Distribution:")
        print("\n+" + "-" * (len(store_ids) + 2 * 6 - 1) + "+")
        print("| Region |", end="")
        for store_id in store_ids:
            print(f" N{store_id}  |", end="")
        print("\n+" + "-" * (len(store_ids) + 2 * 6 - 1) + "+")
        
        # 按照 virtual_region_id 的顺序遍历
        for virtual_region_id in sorted(virtual_region_id_map.keys()):
            real_region_id = virtual_region_id_map[virtual_region_id]
            primary_store_id = region_primary_store_ids.get(real_region_id, None)
            secondary_store_ids = region_secondary_store_ids.get(real_region_id, [])
            
            print(f"| {virtual_region_id:<6} {real_region_id:<6} |", end="")
            for store_id in store_ids:
                if store_id == primary_store_id:
                    print(f" *   |", end="")  # 主副本用 * 表示
                elif store_id in secondary_store_ids:
                    print(f" -   |", end="")  # 从副本用 - 表示
                else:
                    print("     |", end="")  # 没有副本留空
            print("\n+" + "-" * (len(store_ids) + 2 * 6 - 1) + "+")

    def test_print_non_roundrobin_regions(self):
        """
        打印那些不符合 Round-Robin 方式排布的 region。
        Round-Robin 方式要求主副本均匀分布在各个 store_id 上。
        """
    
        # 获取所有 store_id
        store_ids = sorted(self.route.get_all_store_ids())
        num_stores = len(store_ids)
        
        # 获取 virtual_region_id 和 real region_id 的映射关系
        virtual_region_id_map = self.route.virtual_region_id_map
        
        # 获取所有 region 的主副本和从副本信息
        region_primary_store_ids = self.route.region_primary_store_id
        region_secondary_store_ids = self.route.region_secondary_store_id
        
        # 统计每个 store_id 的主副本数量
        store_primary_counts = {store_id: 0 for store_id in store_ids}
        for real_region_id, primary_store_id in region_primary_store_ids.items():
            if primary_store_id in store_primary_counts:
                store_primary_counts[primary_store_id] += 1
        
        # 计算平均每个 store_id 的主副本数量
        total_regions = len(region_primary_store_ids)
        average_primary_count = total_regions / num_stores
        
        # 检查哪些 store_id 的主副本数量明显偏离平均值
        non_roundrobin_store_ids = set()
        for store_id, count in store_primary_counts.items():
            if abs(count - average_primary_count) > 1:  # 允许的偏差为 1
                non_roundrobin_store_ids.add(store_id)
        
        # 打印不符合 Round-Robin 方式的 region
        print("\nRegions Not Following Round-Robin Distribution:")
        print("+" + "-" * 50 + "+")
        print("| Real Region ID | Current       | Expected      |")
        print("+" + "-" * 50 + "+")
        
        for virtual_region_id, real_region_id in sorted(virtual_region_id_map.items()):
            primary_store_id = region_primary_store_ids.get(real_region_id, None)
            secondary_store_ids = region_secondary_store_ids.get(real_region_id, [])
            
            # 如果主副本所在的 store_id 不符合 Round-Robin 方式，则打印该 region
            if primary_store_id in non_roundrobin_store_ids:
                # 当前副本排布
                current_replicas = [f"{store_id}(+)" if store_id == primary_store_id else str(store_id)
                                for store_id in [primary_store_id] + secondary_store_ids]
                current_replicas_str = ",".join(current_replicas)
                
                # 预期的副本排布
                expected_primary_store_id = store_ids[(virtual_region_id) % num_stores]
                expected_secondary_store_ids = [
                    store_ids[(virtual_region_id + i) % num_stores] 
                    for i in range(1, self.route.replica_num)
                ]
                expected_replicas = [f"{store_id}(+)" if store_id == expected_primary_store_id else str(store_id)
                                    for store_id in [expected_primary_store_id] + expected_secondary_store_ids]
                expected_replicas_str = ",".join(expected_replicas)
                
                # 打印结果
                print(f"| {real_region_id:<14} | {current_replicas_str:<13} | {expected_replicas_str:<13} |")
                print("+" + "-" * 50 + "+")

    def test_print_primary_replica_counts(self):
        """
        打印各个 store 拥有的主副本数量。
        """
        # 获取所有 store_id
        store_ids = sorted(self.route.get_all_store_ids())
        
        # 获取所有 region 的主副本信息
        region_primary_store_ids = self.route.region_primary_store_id
        
        # 统计每个 store_id 的主副本数量
        store_primary_counts = {store_id: 0 for store_id in store_ids}
        for real_region_id, primary_store_id in region_primary_store_ids.items():
            if primary_store_id in store_primary_counts:
                store_primary_counts[primary_store_id] += 1
        
        # 打印结果
        print("\nPrimary Replica Counts per Store:")
        print("+" + "-" * 25 + "+")
        print("| Store ID | Primary Count |")
        print("+" + "-" * 25 + "+")
        for store_id, count in store_primary_counts.items():
            print(f"| {store_id:<8} | {count:<13} |")
        print("+" + "-" * 25 + "+")

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