import logging
import grpc
from concurrent import futures
import sql_info_pb2
import sql_info_pb2_grpc
from itertools import combinations
import heapq

# Graph 类：用于构建无向图，维护点权和边权
class Graph:
    def __init__(self, weight=10, theta=1, top_hot_threshold=0):
        """
        初始化图结构。
        :param weight: 不同region之间的边权系数，默认为10
        :param theta: 相同region之间的边权系数，默认为1
        :param top_hot_threshold: 点权阈值，用于筛选top-hot region
        """
        self.adj = {}  # 邻接表，键为regionID，值为相连的regionID集合
        self.node_weights = {}  # 点权，键为regionID，值为出现次数
        self.edge_weights = {}  # 边权，键为frozenset(regionID1, regionID2)，值为边权值
        self.weight = weight  # 不同region之间的边权系数
        self.theta = theta  # 相同region之间的边权系数
        self.top_hot_threshold = top_hot_threshold  # 点权阈值
        self.top_hot_queue = []  # 优先队列，按点权降序排列

    def add_region(self, region_id):
        """
        添加一个新的region到图中。
        :param region_id: 要添加的regionID
        """
        if region_id not in self.node_weights:
            self.node_weights[region_id] = 0  # 初始化点权为0
            self.adj[region_id] = set()  # 初始化邻接表为空集合

    def increment_region(self, region_id):
        """
        增加某个region的点权，并更新优先队列。
        :param region_id: 要增加点权的regionID
        """
        self.add_region(region_id)  # 确保region存在
        self.node_weights[region_id] += 1  # 增加点权
        # 使用负值实现最大堆，按点权降序排列
        heapq.heappush(self.top_hot_queue, (-self.node_weights[region_id], region_id))

    def add_edge(self, region_id1, region_id2):
        """
        在图中添加一条边，并计算边权。
        :param region_id1: 边的第一个regionID
        :param region_id2: 边的第二个regionID
        """
        if region_id1 not in self.adj:
            self.add_region(region_id1)  # 确保region1存在
        if region_id2 not in self.adj:
            self.add_region(region_id2)  # 确保region2存在
        if region_id2 not in self.adj[region_id1]:
            # 添加边到邻接表
            self.adj[region_id1].add(region_id2)
            self.adj[region_id2].add(region_id1)
            # 计算边权
            edge = frozenset({region_id1, region_id2})  # 使用frozenset表示无向边
            if region_id1 == region_id2:
                self.edge_weights[edge] = self.theta  # 相同region的边权为theta
            else:
                self.edge_weights[edge] = self.weight * self.theta  # 不同region的边权为weight * theta

    def get_top_hot_regions(self):
        """
        获取当前点权超过阈值的region列表，按点权降序排列。
        :return: 列表，元素为(regionID, 点权)的元组
        """
        top_regions = []
        temp = []
        # 遍历优先队列，筛选符合条件的region
        while self.top_hot_queue:
            neg_weight, region_id = heapq.heappop(self.top_hot_queue)
            if self.node_weights[region_id] >= self.top_hot_threshold:
                top_regions.append((region_id, -neg_weight))  # 负值转正值
            else:
                heapq.heappush(temp, (neg_weight, region_id))  # 重新放入队列
        # 将未处理的region重新放回队列
        for item in temp:
            heapq.heappush(self.top_hot_queue, item)
        return top_regions

