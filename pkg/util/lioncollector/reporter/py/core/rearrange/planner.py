from core.analyze.clump import Clump
from core.util.route import Route
from core.analyze.graph import Graph
from core.rearrange.subplan import SubPlan

class Planner:
    def __init__(self, route, graph, weight=10, threshold=0.1, batch_size=5):
        self.route = route  # Route对象，包含路由信息
        self.graph = graph  # Graph对象，包含热点信息
        self.weight = weight  # 主副本的权重
        self.threshold = 0.0001  # 负载方差的阈值
        self.batch_size = batch_size  # 每次迁出的clump数量
        self.cache = {}  # 缓存clump的主从store_id和开销

    def evaluate(self, clump, route):
        # 计算clump迁移到各个节点的开销
        # 开销 = - (主副本数 * weight + 从副本数)
        costs = {}
        for store_id in route.get_all_store_ids():
            primary_count = 0
            secondary_count = 0
            for region_id in clump.region_ids:
                primary_store_id = route.get_region_primary_store_id(region_id)
                secondary_store_ids = route.get_region_secondary_store_id(region_id)
                if primary_store_id == store_id:
                    primary_count += 1
                if store_id in secondary_store_ids:
                    secondary_count += 1
            cost = - (primary_count * self.weight + secondary_count)
            costs[store_id] = cost
        return costs

    def generate_subplan(self, hot_clumps):
        # 第一步：选择最小开销的目标节点
        subplans = []
        node_load = {store_id: 0 for store_id in self.route.get_all_store_ids()}
        for clump in hot_clumps:
            if clump in self.cache:
                costs = self.cache[clump]
            else:
                costs = self.evaluate(clump, self.route)
                self.cache[clump] = costs
            # 选择最小开销的节点
            target_store_id = min(costs, key=lambda k: costs[k])
            original_store_ids = []
            for region_id in clump.region_ids:
                primary_store_id = self.route.get_region_primary_store_id(region_id)
                secondary_store_ids = self.route.get_region_secondary_store_id(region_id)
                original_store_ids.extend([primary_store_id] + secondary_store_ids)
            subplan = SubPlan(clump, original_store_ids, target_store_id)
            subplans.append(subplan)
            # 更新节点负载
            node_load[target_store_id] += clump.hot
            clump.target_store_id = target_store_id

        store_load = self.evaluate_load_balance(subplans)
        print("第一阶段load")

        # 计算方差并判断是否需要进行负载均衡调整
        loads = list(node_load.values())
        variance = self.calculate_variance(loads)  # 计算负载方差
        if variance > self.threshold:  # 如果方差大于阈值，则进行负载均衡调整
            # 第二步：负载均衡调整
            adjusted_subplans = set()  # 记录已经调整过的SubPlan
            mean_load = sum(loads) / len(loads)  # 计算负载均值
            # 找到所有负载高于均值的节点
            overloaded_nodes = [store_id for store_id, load in sorted(node_load.items(), key=lambda x: x[1], reverse=True) if load > mean_load]

            round = 0
            while(True):
                round += 1
                loads = list(node_load.values())
                variance = self.calculate_variance(loads)  # 计算负载方差
                print("调整轮数：", str(round), " 当前方差：", str(variance), " ", str(self.threshold), " 当前均值：", mean_load)
                if variance < self.threshold or len(overloaded_nodes) == 0:  # 如果方差小于阈值
                    break
                # 对每个超载节点进行调整
                for max_load_store in overloaded_nodes:
                    # 找到负载最低的节点
                    min_load_store = min(node_load, key=lambda k: node_load[k])
                    # 从超载节点迁出batch_size个clump到负载最低的节点
                    clumps_to_move = [
                        subplan for subplan in subplans
                        if subplan.target_store_id == max_load_store and subplan not in adjusted_subplans
                    ]
                    clumps_to_move = clumps_to_move[:self.batch_size]
                    for subplan in clumps_to_move:
                        # 迁出clump
                        node_load[max_load_store] -= subplan.clump.hot
                        # 迁入clump
                        node_load[min_load_store] += subplan.clump.hot
                        # 更新subplan的目标store_id
                        subplan.target_store_id = min_load_store
                        # 标记为已调整
                        adjusted_subplans.add(subplan)
                        if node_load[max_load_store] <= mean_load or node_load[min_load_store] >= mean_load: 
                            break
                    
                    # 如果当前节点负载已经低于均值，将其从超载节点列表中移除
                    if node_load[max_load_store] <= mean_load:
                        overloaded_nodes.remove(max_load_store)

                if len(overloaded_nodes) == 0:
                    overloaded_nodes = [store_id for store_id, load in sorted(node_load.items(), key=lambda x: x[1], reverse=True) if load > mean_load * 1]
        store_load = self.evaluate_load_balance(subplans)
        print("第二阶段load: ", store_load)

        return subplans

    # def calculate_variance(self, loads):
    #     # 计算负载的方差
    #     mean = sum(loads) / len(loads)
    #     variance = sum((x - mean) ** 2 for x in loads) / len(loads)
    #     return variance

    def calculate_variance(self, loads):
        # 归一化负载（使用总和作为分母）
        total_load = sum(loads) if sum(loads) != 0 else 1  # 防止除零错误
        normalized_loads = [load / total_load for load in loads]
        
        # 计算归一化后的负载方差
        mean = sum(normalized_loads) / len(normalized_loads)
        variance = sum((x - mean) ** 2 for x in normalized_loads) / len(normalized_loads)
        print(f"   负载：{loads} \n   归一化负载 : {normalized_loads}, \n   方差: {variance}")
        return variance

    def evaluate_load_balance(self, subplans):
        """
        评估当前Planner生成计划的负载均衡性。
        :param subplans: SubPlan列表，表示生成的迁移计划
        :return: 一个字典，key是store_id，value是该节点被分配到的负载量
        """
        store_load = {}  # 初始化store负载映射
        for subplan in subplans:
            target_store_id = subplan.target_store_id
            clump_weight = subplan.clump.hot
            # 累加目标节点的负载
            if target_store_id in store_load:
                store_load[target_store_id] += clump_weight
            else:
                store_load[target_store_id] = clump_weight
        return store_load