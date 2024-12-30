import time
from core.rearrange.opplan import OpPlan
import subprocess

class Adaptor:
    def __init__(self, pd_api_url, route, mock=False):
        """
        初始化Adaptor模块。
        
        :param pd_api_url: PD API的URL地址
        :param route: Route对象，包含路由信息
        """
        self.pd_api_url = pd_api_url
        self.route = route
        self.mock = mock
        self.pd_command_prefix = f"tiup ctl:v8.5.0 pd -u {pd_api_url}"

    def generate_op_plan(self, subplans):
        """
        将subplan转换为对应的operator命令的HTTP请求描述。
        
        :param subplans: SubPlan列表
        :return: 包含所有OpPlan对象的列表
        """
        op_plans = []
        
        for index, subplan in enumerate(subplans):
            clump = subplan.clump
            target_store_id = subplan.target_store_id
            
            for virtual_region_id in clump.region_ids:
                actual_region_id = self.route.virtual_region_id_map[virtual_region_id]
                primary_store_id = self.route.get_region_primary_store_id(virtual_region_id)
                secondary_store_ids = self.route.get_region_secondary_store_id(virtual_region_id)
                
                op_plan = OpPlan(index)
                
                if target_store_id == primary_store_id:
                    # 目标store已经是主节点，生成空的OpPlan
                    op_plans.append(op_plan)
                    continue
                
                elif target_store_id in secondary_store_ids:
                    # 生成transfer_leader命令
                    op = {
                        "operator": "transfer_leader",
                        "region_id": actual_region_id,
                        "target_store": target_store_id
                    }
                    op_plan.add_op(op)
                
                else:
                    # 生成transfer_peer命令
                    if len(secondary_store_ids) == 0:
                        # 没有从节点，无法转移，生成空的OpPlan
                        # todo add peer
                        op_plans.append(op_plan)
                        continue
                    
                    from_store_id = secondary_store_ids[0]
                    transfer_peer_op = {
                        "operator": "transfer_peer",
                        "region_id": actual_region_id,
                        "from_store": from_store_id,
                        "to_store": target_store_id
                    }
                    op_plan.add_op(transfer_peer_op)
                    
                    # 生成transfer_leader命令
                    transfer_leader_op = {
                        "operator": "transfer_leader",
                        "region_id": actual_region_id,
                        "target_store": target_store_id
                    }
                    op_plan.add_op(transfer_leader_op)
                
                op_plans.append(op_plan)
        
        return op_plans

    def do_operator_plan(self, op_plans):
        """
        发送operator计划到PD，并记录和打印每次请求的延迟。
        
        :param op_plans: 包含所有OpPlan对象的列表
        :param mock: 如果为True，只打印请求，不实际发送
        """
        for op_plan in op_plans:
            if op_plan.is_empty():
                print(f"No operator for SubPlan index {op_plan.subplan_index}")
                continue
            
            for op in op_plan.op_str:
                # 根据operator类型生成对应的命令
                operator_type = op["operator"]
                region_id = op["region_id"]
                if operator_type == "transfer_leader":
                    target_store = op["target_store"]
                    command = f"{self.pd_command_prefix} operator add transfer-leader {region_id} {target_store}"
                elif operator_type == "transfer_peer":
                    from_store = op["from_store"]
                    to_store = op["to_store"]
                    command = f"{self.pd_command_prefix} operator add transfer-peer {region_id} {from_store} {to_store}"
                elif operator_type == "add_peer":
                    target_store = op["target_store"]
                    command = f"{self.pd_command_prefix} operator add add-peer {region_id} {target_store}"
                elif operator_type == "remove_peer":
                    target_store = op["target_store"]
                    command = f"{self.pd_command_prefix} operator add remove-peer {region_id} {target_store}"
                else:
                    print(f"Unknown operator type: {operator_type}")
                    continue
                
                if self.mock:
                    print(f"Mock request: {command}")
                    continue
                
                start_time = time.time()
                try:
                    # 使用subprocess调用命令
                    result = subprocess.run(command, shell=True, capture_output=True, text=True)
                    end_time = time.time()
                    latency = end_time - start_time
                    
                    if result.returncode == 0:
                        print(f"Sent operator: {command}, latency: {latency:.2f} seconds, response: {result.stdout}")
                    else:
                        print(f"Failed to send operator: {command}, error: {result.stderr}")
                except Exception as e:
                    print(f"Error sending operator: {command}, exception: {e}")