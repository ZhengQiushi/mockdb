class OpPlan:
    def __init__(self, subplan_index, op_str=None):
        """
        初始化OpPlan对象。
        
        :param subplan_index: 对应的SubPlan的index
        :param op_str: operator命令的HTTP请求描述，默认为None
        """
        self.subplan_index = subplan_index
        self.op_str = op_str if op_str else []

    def add_op(self, op):
        """
        添加一个operator命令。
        
        :param op: operator命令的HTTP请求描述
        """
        self.op_str.append(op)

    def is_empty(self):
        """
        判断OpPlan是否为空。
        
        :return: 如果op_str为空，返回True；否则返回False
        """
        return len(self.op_str) == 0