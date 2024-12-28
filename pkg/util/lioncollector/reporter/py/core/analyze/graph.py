from core.analyze.vertex import Vertex
from core.analyze.edge import Edge
from core.analyze.clump import Clump
import heapq
from itertools import combinations
from collections import deque

class Graph:
    def __init__(self, weight=10, theta=1, top_hot_threshold=0):
        """
        初始化图结构。
        :param weight: 不同region之间的边权系数，默认为10
        :param theta: 相同region之间的边权系数，默认为1
        :param top_hot_threshold: 点权阈值，用于筛选top-hot region
        """
        self.vertices = {}  # 顶点集合，键为regionID，值为Vertex对象
        self.edges = {}  # 边集合，键为frozenset(regionID1, regionID2)，值为Edge对象
        self.weight = weight  # 不同region之间的边权系数
        self.theta = theta  # 相同region之间的边权系数
        self.top_hot_threshold = top_hot_threshold  # 点权阈值
        self.top_hot_queue = []  # 优先队列，按点权降序排列

    def add_vertex(self, region_id):
        """
        添加一个新的顶点到图中。
        :param region_id: 要添加的regionID
        """
        if region_id not in self.vertices:
            self.vertices[region_id] = Vertex(region_id)

    def increment_vertex_weight(self, region_id, value=1):
        """
        增加某个顶点的点权，并更新优先队列。
        :param region_id: 要增加点权的regionID
        :param value: 增加的值，默认为1
        """
        self.add_vertex(region_id)
        self.vertices[region_id].increment_weight(value)
        # 使用负值实现最大堆，按点权降序排列
        heapq.heappush(self.top_hot_queue, (-self.vertices[region_id].weight, region_id))

    def add_edge(self, region_id1, region_id2, weight=1):
        """
        在图中添加一条边，并更新邻接表。
        :param region_id1: 边的第一个regionID
        :param region_id2: 边的第二个regionID
        """
        self.add_vertex(region_id1)
        self.add_vertex(region_id2)
        theta = self.theta * weight
        edge_key = frozenset({region_id1, region_id2})
        if edge_key not in self.edges:
            if region_id1 == region_id2:
                self.edges[edge_key] = Edge(region_id1, region_id2, theta)
            else:
                self.edges[edge_key] = Edge(region_id1, region_id2, self.weight * theta)
            # 更新邻接表
            self.vertices[region_id1].add_adjacent_region(region_id2)
            self.vertices[region_id2].add_adjacent_region(region_id1)
        else:
            self.edges[edge_key].increment_weight(theta if region_id1 == region_id2 else self.weight * theta)

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
            if self.vertices[region_id].weight >= self.top_hot_threshold:
                top_regions.append((region_id, -neg_weight))  # 负值转正值
            else:
                heapq.heappush(temp, (neg_weight, region_id))  # 重新放入队列
        # 将未处理的region重新放回队列
        for item in temp:
            heapq.heappush(self.top_hot_queue, item)
        return top_regions

    def get_adjacent_regions(self, region_id):
        """
        获取某个region的相邻region集合。
        :param region_id: 目标regionID
        :return: 相邻regionID的集合
        """
        if region_id in self.vertices:
            return self.vertices[region_id].get_adjacent_regions()
        return set()
    
    def get_hot_region(self, edge_thresh):
        """
        获取当前图中的热点闭包。
        :param edge_thresh: 边权阈值，大于该值的边才会被认定为高关联
        :return: 一个列表，每一项是一个Clump对象，表示一个热点闭包
        """
        visited = set()  # 缓存已经处理过的regionID
        hot_clumps = []  # 存储所有热点闭包

        # 遍历top_hot_queue中的regionID
        while self.top_hot_queue:
            neg_weight, region_id = heapq.heappop(self.top_hot_queue)
            if region_id in visited:
                continue  # 如果已经处理过，跳过
            # 初始化当前闭包的regionID集合和总点权
            clump_region_ids = set()
            clump_hot = 0
            # 使用BFS进行扩散
            queue = deque([region_id])
            while queue:
                current_region = queue.popleft()
                if current_region in visited:
                    continue  # 如果已经处理过，跳过
                visited.add(current_region)
                clump_region_ids.add(current_region)
                clump_hot += self.vertices[current_region].weight
                # 遍历相邻节点
                for neighbor in self.vertices[current_region].get_adjacent_regions():
                    edge_key = frozenset({current_region, neighbor})
                    if edge_key in self.edges and self.edges[edge_key].weight > edge_thresh and neighbor not in visited:
                        queue.append(neighbor)
            # 将当前闭包添加到结果中
            if clump_region_ids:
                hot_clumps.append(Clump(clump_region_ids, clump_hot))
        return hot_clumps
    


    def add_transaction(self, region_ids, weight=1):
        """
        添加一个事务，更新点权和边权。
        :param region_ids: 事务访问的regionID列表
        :param weight: 事务的权重，默认为1
        """
        # 更新点权
        for region_id in region_ids:
            self.increment_vertex_weight(region_id, weight)
        # 更新边权
        for region_pair in combinations(region_ids, 2):
            if region_pair[0] != region_pair[1]:
                self.add_edge(region_pair[0], region_pair[1], weight)