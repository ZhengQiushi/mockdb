from typing import Dict, List, Set
import subprocess
import json
import pickle

class Route:
    def __init__(self):
        """
        初始化 Route 模块。
        """
        self.store_ids: Set[int] = set()  # 所有 store_id
        self.region_primary_store_id: Dict[int, int] = {}  # 实际 region_id -> 主节点 store_id
        self.region_secondary_store_id: Dict[int, List[int]] = {}  # 实际 region_id -> 从节点 store_id 列表
        self.virtual_region_id_map: Dict[int, int] = {}  # 虚拟 region_id -> 实际 region_id

    def update_region(self, data: Dict):
        """
        更新路由信息。
        :param data: 包含region信息的JSON数据
        """
        regions = data.get("record_regions", [])
        self.virtual_region_id_map = {}
        self.region_primary_store_id = {}   # actual_id -> store_id
        self.region_secondary_store_id = {} # actual_id -> store_id
        self.store_ids = set()

        for virtual_id, region in enumerate(regions):
            actual_id = region["region_id"]
            self.virtual_region_id_map[virtual_id] = actual_id

            leader = region["leader"]
            peers = region["peers"]

            # 更新主节点
            self.region_primary_store_id[actual_id] = leader["store_id"]

            # 更新从节点
            secondary_store_ids = [peer["store_id"] for peer in peers if peer["id"] != leader["id"]]
            self.region_secondary_store_id[actual_id] = secondary_store_ids

            # 更新所有 store_id
            self.store_ids.add(leader["store_id"])
            for peer in peers:
                self.store_ids.add(peer["store_id"])

    def update_region_from_pd(self, pd_url: str):
        """
        从 PD 获取 region 信息并更新路由表。
        :param pd_url: PD 的 URL，例如 "http://10.77.70.205:10080/tables/benchbase/usertable/regions"
        """
        # 使用 curl 命令获取数据
        try:
            result = subprocess.run(
                ["curl", "-s", pd_url],  # -s 参数表示静默模式
                capture_output=True,
                text=True,
                check=True
            )
            data = json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to fetch region info from PD: {e.stderr}")
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse JSON response: {e}")

        self.update_region(data)

    def get_region_primary_store_id(self, virtual_region_id: int) -> int:
        """
        获取某个虚拟 region 的主节点 store_id。
        :param virtual_region_id: 虚拟 region_id
        :return: 主节点 store_id
        """
        assert virtual_region_id in self.virtual_region_id_map, f"虚拟 region_id {virtual_region_id} 不存在"
        actual_region_id = self.virtual_region_id_map[virtual_region_id]
        assert actual_region_id in self.region_primary_store_id, f"实际 region_id {actual_region_id} 没有主节点信息"
        return self.region_primary_store_id[actual_region_id]

    def get_region_secondary_store_id(self, virtual_region_id: int) -> List[int]:
        """
        获取某个虚拟 region 的从节点 store_id 列表。
        :param virtual_region_id: 虚拟 region_id
        :return: 从节点 store_id 列表
        """
        assert virtual_region_id in self.virtual_region_id_map, f"虚拟 region_id {virtual_region_id} 不存在"
        actual_region_id = self.virtual_region_id_map[virtual_region_id]
        assert actual_region_id in self.region_secondary_store_id, f"实际 region_id {actual_region_id} 没有从节点信息"
        return self.region_secondary_store_id[actual_region_id]

    def get_all_store_ids(self) -> Set[int]:
        """
        获取所有 store_id。
        :return: 所有 store_id 的集合
        """
        return self.store_ids

    def save(self, filename):
        """
        将当前 Route 对象保存到文件中。
        :param filename: 保存的文件名
        """
        with open(filename, 'wb') as file:
            pickle.dump(self, file)

    @staticmethod
    def load(filename):
        """
        从文件中加载 Route 对象。
        :param filename: 保存的文件名
        :return: 加载的 Route 对象
        """
        with open(filename, 'rb') as file:
            route = pickle.load(file)
        return route