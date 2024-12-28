import unittest
import json
from core.util.route import Route

class TestRoute(unittest.TestCase):

    def setUp(self):
        self.route = Route()

    def test_update_region_from_pd(self):
        # 调用 update_region_from_pd
        self.route.update_region_from_pd("http://10.77.110.148:10080/tables/benchbase/usertable/regions")


    def test_update_region_from_pd_mock(self):
        mock_response = {
            "name": "usertable",
            "id": 112,
            "record_regions": [
                {
                    "region_id": 2005,
                    "leader": {"id": 2007, "store_id": 7},
                    "peers": [
                        {"id": 2006, "store_id": 2},
                        {"id": 2007, "store_id": 7},
                        {"id": 2008, "store_id": 1}
                    ],
                    "region_epoch": {"conf_ver": 5, "version": 71}
                },
                {
                    "region_id": 2009,
                    "leader": {"id": 2011, "store_id": 7},
                    "peers": [
                        {"id": 2010, "store_id": 2},
                        {"id": 2011, "store_id": 7},
                        {"id": 2012, "store_id": 1}
                    ],
                    "region_epoch": {"conf_ver": 5, "version": 67}
                }
            ],
            "indices": []
        }

        # 调用 update_region_from_pd
        self.route.update_region(mock_response)

        # 检查 store_ids
        self.assertEqual(self.route.get_all_store_ids(), {1, 2, 7})

        # 检查 region_primary_store_id
        self.assertEqual(self.route.get_region_primary_store_id(2005), 7)
        self.assertEqual(self.route.get_region_primary_store_id(2009), 7)

        # 检查 region_secondary_store_id
        self.assertEqual(self.route.get_region_secondary_store_id(2005), [2, 1])
        self.assertEqual(self.route.get_region_secondary_store_id(2009), [2, 1])

    def test_get_region_primary_store_id_not_found(self):
        # 测试获取不存在的 region_id 的主节点
        self.assertIsNone(self.route.get_region_primary_store_id(9999))

    def test_get_region_secondary_store_id_not_found(self):
        # 测试获取不存在的 region_id 的从节点
        self.assertEqual(self.route.get_region_secondary_store_id(9999), [])

if __name__ == '__main__':
    unittest.main()