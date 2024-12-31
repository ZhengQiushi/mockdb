import time
import json
from collections import deque

class OpPlan:
    def __init__(self, subplan_index, region_id, op_str=None):
        """
        初始化OpPlan对象。
        
        :param subplan_index: 对应的SubPlan的index
        :param op_str: operator命令的HTTP请求描述，默认为None
        """
        self.subplan_index = subplan_index
        self.region_id = region_id
        self.op_str = op_str if op_str else []
        self.op_str_status = [False] * len(self.op_str)
        self.next_retry_time = None
        self.retry_count = 0  # 重试次数

    def mark_op_str_as_success(self, index):
        """
        将指定index的op_str标记为执行成功（设置为1）。
        
        :param index: op_str的索引
        """
        if 0 <= index < len(self.op_str_status):
            self.op_str_status[index] = True
        else:
            raise IndexError("Index out of range for op_str_status.")
        
    def add_op(self, op):
        """
        添加一个operator命令。
        
        :param op: operator命令的HTTP请求描述
        """
        self.op_str.append(op)
        self.op_str_status.append(False)

    def is_empty(self):
        """
        判断OpPlan是否为空。
        
        :return: 如果op_str为空，返回True；否则返回False
        """
        return len(self.op_str) == 0