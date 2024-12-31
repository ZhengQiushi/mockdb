import time
import json
import subprocess
from collections import deque
from core.rearrange.opplan import OpPlan
import re

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
        self.retry_queue = deque()  # 重试队列
        self.MAX_RETRY = 5  # 最大重试次数
        self.retry_interval = 10

    def generate_op_plan(self, actual_region_id, primary_store_id, secondary_store_ids, target_store_id, op_index):
        '''
        
        '''
        op_plan = OpPlan(op_index, actual_region_id)
        
        if target_store_id == primary_store_id:
            # 目标store已经是主节点，生成空的OpPlan
            pass 
        
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
                pass
            else:
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
        return op_plan
 
    def generate_op_plans(self, subplans):
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
                
                op_plan = self.generate_op_plan(
                    actual_region_id, 
                    primary_store_id, 
                    secondary_store_ids, 
                    target_store_id, 
                    index)
                op_plans.append(op_plan)
        
        return op_plans

    def do_operator_plan(self, op_plans, mock=False):
        """
        发送operator计划到PD，并记录和打印每次请求的延迟。
        
        :param op_plans: 包含所有OpPlan对象的列表
        :param mock: 如果为True，只打印请求，不实际发送
        """
        self.op_plans = deque(op_plans)  # 使用deque进行高效的操作
        while self.op_plans and len(self.op_plans) > 0:
            op_plan = self.op_plans.popleft()
            # 检查重试时间
            if op_plan.next_retry_time and op_plan.next_retry_time > time.time():
                time.sleep(max(0, op_plan.next_retry_time - time.time()))
            # 检查重试次数
            if op_plan.retry_count >= self.MAX_RETRY:
                print(f"OpPlan {op_plan.subplan_index} - {op_plan.region_id} 已达到最大重试次数，跳过。")
                continue
            if op_plan.is_empty():
                print(f"No operator for SubPlan index {op_plan.subplan_index} - {op_plan.region_id}")
                continue
            
            for index, op in enumerate(op_plan.op_str):
                if(op_plan.op_str_status[index] == True):
                    print(f"OpPlan {op_plan.subplan_index} - {op_plan.region_id} operator done before, skip: {op}")
                    continue
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

                    if "Fail" in result.stdout or "500" in result.stdout:
                        print(f" OpPlan {op_plan.subplan_index} - {op_plan.region_id} operator failed: {result.stdout} retry: {op_plan.retry_count}")
                        self.handle_error(op_plan, result, region_id, command)
                        # op_plans.append(op_plan)  # 失败的 op_plan 重新加入队列
                    else:
                        # if result.returncode == 0:
                        op_plan.mark_op_str_as_success(index)
                        print(f"OpPlan {op_plan.subplan_index} - {op_plan.region_id} sent operator: {command}, latency: {latency:.2f} seconds, response: {result.stdout}")
                except Exception as e:
                    print(f"Error sending operator: {command}, exception: {e}")
                    self.op_plans.append(op_plan)  # 发生异常时重新加入队列

    def handle_error(self, op_plan, result, region_id, command):
        """
        处理操作命令失败的情况。
        
        :param op_plan: 失败的OpPlan对象
        :param result: subprocess执行结果
        :param region_id: region ID
        :param command: 执行的命令
        """
        error_msg = result.stderr.lower() + result.stdout.lower()  # 合并 stderr 和 stdout
        print(f"Debug: Full error message: {error_msg}")  # 打印完整错误信息，方便调试
        pending = re.search(r"region has no voter in store", error_msg)
        # 使用正则表达式匹配错误信息
        if pending and op_plan.retry_count < 1:
            print(f"Region has no voter in store, retrying OpPlan {op_plan.subplan_index} - {op_plan.region_id} after {self.retry_interval} seconds.")
            op_plan.next_retry_time = time.time() + self.retry_interval
            op_plan.retry_count += 1
            self.op_plans.append(op_plan)
        elif pending or re.search(r"no operator step is built", error_msg) or re.search(r"region has no peer in store", error_msg):
            print(f"No operator step is built for OpPlan {op_plan.subplan_index} - {op_plan.region_id}, checking region peers.")
            self.check_region_peers(op_plan, region_id)
        else:
            print(f"Unknown error for OpPlan {op_plan.subplan_index} - {op_plan.region_id}: {error_msg}")

    def check_region_peers(self, op_plan, region_id):
        """
        通过curl命令检查region的peer分布情况。
        
        :param op_plan: 失败的OpPlan对象
        :param region_id: region ID
        :param command: 执行的命令
        """
        pd_url = f"{self.pd_api_url}/pd/api/v1/region/id/{region_id}"
        try:
            # 使用subprocess.run调用curl命令，以列表形式传递参数
            result = subprocess.run(
                ["curl", "-s", pd_url],  # -s 参数表示静默模式
                capture_output=True,
                text=True,
                check=True  # 如果命令返回非零状态码，抛出CalledProcessError
            )
            
            # 解析curl返回的JSON数据
            region = json.loads(result.stdout)
            
            print(region)

            leader = region["leader"]
            peers = region["peers"]

            # 更新主节点
            leader_store_id = leader["store_id"]

            # 更新从节点
            secondary_store_ids = [peer["store_id"] for peer in peers if peer["id"] != leader["id"]]
            
            # 获取目标store ID
            target_store_id = op_plan.op_str[0].get("to_store", None) if op_plan.op_str else None
            
            if target_store_id == leader_store_id:
                # 检查目标store是否已经是leader
                print(f"Target store {target_store_id} is already the leader, skipping.")
            elif target_store_id in secondary_store_ids and any(
                    peer["store_id"] == target_store_id and peer.get("role_name") == "Learner" 
                    for peer in peers
                ):
                # 检查目标store是否已经存在peer，但仍然是Learner
                print(f"Target store {target_store_id} is still Learner, pending and retry.")
                op_plan.next_retry_time = time.time() + self.retry_interval  # 设置重试时间
                op_plan.retry_count = op_plan.retry_count + 1  # 增加重试次数
                self.op_plans.append(op_plan)  # 添加到重试队列
            else:
                print(f"Target store {target_store_id} is not the leader, re-generating op_plan.")
                
                # 重新生成op_plan并添加到重试队列
                new_op_plan = self.generate_op_plan(region_id, leader_store_id, secondary_store_ids, target_store_id, op_plan.subplan_index)

                new_op_plan.next_retry_time = time.time() + self.retry_interval  # 设置重试时间
                new_op_plan.retry_count = op_plan.retry_count + 1  # 增加重试次数
                self.op_plans.append(new_op_plan)  # 添加到重试队列
        
        except subprocess.CalledProcessError as e:
            # 处理curl命令执行失败的情况
            print(f"Failed to fetch region info from PD: {e.stderr} cmd: {pd_url}")
        except json.JSONDecodeError as e:
            # 处理JSON解析错误
            print(f"Failed to parse JSON response for region {region_id}: {e}")
        except Exception as e:
            # 处理其他异常
            print(f"Error checking region peers: {e}")