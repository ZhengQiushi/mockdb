import threading

class Edge:
    def __init__(self, region_id1, region_id2, weight=1):
        """
        初始化边。
        :param region_id1: 边的第一个regionID
        :param region_id2: 边的第二个regionID
        :param weight: 边的权重，默认为1
        """
        self.region_id1 = region_id1
        self.region_id2 = region_id2
        self.weight = weight
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
        增加边权。
        :param value: 增加的值，默认为1
        """
        with self.lock:
            self.weight += value