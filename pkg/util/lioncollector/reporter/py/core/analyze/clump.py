class Clump:
    def __init__(self, region_ids, hot):
        """
        初始化热点闭包。
        :param region_ids: 热点闭包中的regionID集合
        :param hot: 热点闭包的总点权
        """
        self.region_ids = region_ids
        self.hot = hot

    def __repr__(self):
        return f"Clump(region_ids={self.region_ids}, hot={self.hot})"