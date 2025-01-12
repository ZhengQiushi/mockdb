import threading

class Vertex:
    def __init__(self, region_id):
        """
        初始化顶点。
        :param region_id: 顶点的regionID
        """
        self.region_id = region_id
        self.weight = 0  # 点权，初始为0
        self.adjacent_regions = set()  # 邻接表，存储与该顶点相连的regionID集合
        self.lock = threading.Lock()  # 线程锁

    def __getstate__(self):
        # 序列化时排除线程锁
        state = self.__dict__.copy()
        del state['lock']
        return state

    def __setstate__(self, state):
        # 反序列化时恢复状态并重新初始化线程锁
        self.__dict__.update(state)
        self.lock = threading.Lock()  # 重新初始化线程锁


    def increment_weight(self, value=1):
        """
        增加点权。
        :param value: 增加的值，默认为1
        """
        with self.lock:
            self.weight += value

    def add_adjacent_region(self, region_id):
        """
        添加一个相邻的regionID到邻接表中。
        :param region_id: 相邻的regionID
        """
        with self.lock:
            self.adjacent_regions.add(region_id)

    def get_adjacent_regions(self):
        """
        获取与该顶点相连的所有regionID集合。
        :return: 相邻regionID的集合
        """
        with self.lock:
            return self.adjacent_regions.copy()  # 返回副本以避免外部修改