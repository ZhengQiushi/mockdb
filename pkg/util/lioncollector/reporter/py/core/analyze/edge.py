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

    def increment_weight(self, value=1):
        """
        增加边权。
        :param value: 增加的值，默认为1
        """
        self.weight += value