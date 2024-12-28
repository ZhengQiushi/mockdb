from typing import Dict, List, Set
import subprocess
import json

class Route:
    def __init__(self):
        """
        初始化 Route 模块。
        """
        self.store_ids: Set[int] = set()  # 所有 store_id
        self.region_primary_store_id: Dict[int, int] = {}  # region_id -> 主节点 store_id
        self.region_secondary_store_id: Dict[int, List[int]] = {}  # region_id -> 从节点 store_id 列表

    def update_region(self, data: json):
        for region in data["record_regions"]:
            region_id = region["region_id"]
            leader = region["leader"]
            peers = region["peers"]
            
            # 更新主节点
            self.region_primary_store_id[region_id] = leader["store_id"]
            
            # 更新从节点
            secondary_store_ids = [peer["store_id"] for peer in peers if peer["id"] != leader["id"]]
            self.region_secondary_store_id[region_id] = secondary_store_ids
            
            # 更新所有 store_id
            self.store_ids.add(leader["store_id"])
            for peer in peers:
                self.store_ids.add(peer["store_id"])

    def update_region_from_pd(self, pd_url: str):
        """
        从 PD 获取 region 信息并更新路由表。
        :param pd_url: PD 的 URL，例如 "http://10.77.110.148:10080/tables/benchbase/usertable/regions"
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

    def get_region_primary_store_id(self, region_id: int) -> int:
        """
        获取某个 region 的主节点 store_id。
        :param region_id: 目标 region_id
        :return: 主节点 store_id
        """
        return self.region_primary_store_id.get(region_id)

    def get_region_secondary_store_id(self, region_id: int) -> List[int]:
        """
        获取某个 region 的从节点 store_id 列表。
        :param region_id: 目标 region_id
        :return: 从节点 store_id 列表
        """
        return self.region_secondary_store_id.get(region_id, [])

    def get_all_store_ids(self) -> Set[int]:
        """
        获取所有 store_id。
        :return: 所有 store_id 的集合
        """
        return self.store_ids