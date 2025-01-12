import time
import json
import subprocess
from collections import deque
from core.rearrange.opplan import OpPlan
import re
import queue
from concurrent.futures import ThreadPoolExecutor
import threading  # 导入 threading 模块以获取线程 ID

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
        self.MAX_RETRY = 10  # 最大重试次数
        self.retry_interval = 20
        self.max_threads = 20
        self.op_plans = queue.Queue()  # 使用线程安全的队列
        self.atomic_counter = 0  # 原子计数器，用于跟踪未完成的操作计划数量
        self.counter_lock = threading.Lock()  # 用于保护计数器的锁

    def generate_op_plan(self, actual_region_id, primary_store_id, secondary_store_ids, target_store_id, op_index):
        '''
        生成操作计划。
        
        :param actual_region_id: 实际的region ID
        :param primary_store_id: 主节点store ID
        :param secondary_store_ids: 从节点store ID列表
        :param target_store_id: 目标store ID
        :param op_index: 操作索引
        :return: 生成的OpPlan对象
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
                "to_store": target_store_id
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
                    "to_store": target_store_id
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

    def process_op_plan(self, op_plan):
        """
        处理单个操作计划。
        
        :param op_plan: 要处理的操作计划
        """
        thread_id = threading.get_ident()  # 获取当前线程 ID
        is_done = True
        # 检查重试时间
        if op_plan.next_retry_time and op_plan.next_retry_time > time.time():
            time.sleep(max(0, op_plan.next_retry_time - time.time()))
        # 检查重试次数
        if op_plan.retry_count >= self.MAX_RETRY:
            print(f"[Thread-{thread_id}] OpPlan {op_plan.subplan_index} - {op_plan.region_id} 已达到最大重试次数，跳过。")
            pass
        elif op_plan.is_empty():
            print(f"[Thread-{thread_id}] No operator for SubPlan index {op_plan.subplan_index} - {op_plan.region_id}")
            pass
        else: 
            for index, op in enumerate(op_plan.op_str):
                if op_plan.op_str_status[index] == True:
                    print(f"[Thread-{thread_id}] OpPlan {op_plan.subplan_index} - {op_plan.region_id} - {index} operator done before, skip: {op}")
                    continue
                # 根据operator类型生成对应的命令
                operator_type = op["operator"]
                region_id = op["region_id"]
                if operator_type == "transfer_leader":
                    to_store = op["to_store"]
                    command = f"{self.pd_command_prefix} operator add transfer-leader {region_id} {to_store}"
                elif operator_type == "transfer_peer":
                    from_store = op["from_store"]
                    to_store = op["to_store"]
                    command = f"{self.pd_command_prefix} operator add transfer-peer {region_id} {from_store} {to_store}"
                elif operator_type == "add_peer":
                    to_store = op["to_store"]
                    command = f"{self.pd_command_prefix} operator add add-peer {region_id} {to_store}"
                elif operator_type == "remove_peer":
                    to_store = op["to_store"]
                    command = f"{self.pd_command_prefix} operator add remove-peer {region_id} {to_store}"
                else:
                    print(f"[Thread-{thread_id}] Unknown operator type: {operator_type}")
                    continue
                
                if self.mock:
                    print(f"[Thread-{thread_id}] Mock request {op_plan.subplan_index} - {op_plan.region_id} - {index} : {command}")
                    continue
                
                start_time = time.time()
                try:
                    # 使用subprocess调用命令
                    result = subprocess.run(command, shell=True, capture_output=True, text=True)
                    end_time = time.time()
                    latency = end_time - start_time

                    if "Fail" in result.stdout or "500" in result.stdout:
                        print(f"[Thread-{thread_id}] OpPlan {op_plan.subplan_index} - {op_plan.region_id} - {index} operator {command} failed: {result.stdout} retry: {op_plan.retry_count}")
                        self.handle_error(op_plan, result, region_id, command)
                        is_done = False
                        break
                    else:
                        # if result.returncode == 0:
                        op_plan.mark_op_str_as_success(index)
                        print(f"[Thread-{thread_id}] OpPlan {op_plan.subplan_index} - {op_plan.region_id} sent operator: {command}, latency: {latency:.2f} seconds, response: {result.stdout}")
                except Exception as e:
                    print(f"[Thread-{thread_id}] Error sending operator: {command}, exception: {e}")
                    self.op_plans.put(op_plan)  # 发生异常时重新加入队列
                    break  # 退出当前操作计划的处理
            
        # 操作计划处理完成后，减少计数器
        if is_done == True:
            with self.counter_lock:
                self.atomic_counter -= 1
                print(f"[Thread-{thread_id}] Done OpPlan {op_plan.subplan_index} - {op_plan.region_id} sent operator: {command} self.atomic_counter: {self.atomic_counter}" )

    def handle_error(self, op_plan, result, region_id, command):
        """
        处理操作命令失败的情况。
        
        :param op_plan: 失败的OpPlan对象
        :param result: subprocess执行结果
        :param region_id: region ID
        :param command: 执行的命令
        """
        thread_id = threading.get_ident()  # 获取当前线程 ID
        error_msg = result.stderr.lower() + result.stdout.lower()  # 合并 stderr 和 stdout
        print(f"[Thread-{thread_id}] Debug: Full error message: {error_msg}")  # 打印完整错误信息，方便调试
        pending = re.search(r"region has no voter in store", error_msg)
        # 使用正则表达式匹配错误信息
        if pending and op_plan.retry_count < 1:
            print(f"[Thread-{thread_id}] Region has no voter in store, retrying OpPlan {op_plan.subplan_index} - {op_plan.region_id} after {self.retry_interval} seconds.")
            op_plan.next_retry_time = time.time() + self.retry_interval
            op_plan.retry_count += 1
            self.op_plans.put(op_plan)  # 重新加入队列
        elif pending or re.search(r"no operator step is built", error_msg) or re.search(r"region has no peer in store", error_msg):
            print(f"[Thread-{thread_id}] No operator step is built for OpPlan {op_plan.subplan_index} - {op_plan.region_id}, checking region peers.")
            self.check_region_peers(op_plan, region_id)
        else:
            print(f"[Thread-{thread_id}] Unknown error for OpPlan {op_plan.subplan_index} - {op_plan.region_id}: {error_msg}")
            self.check_region_peers(op_plan, region_id)

    def check_region_peers(self, op_plan, region_id):
        """
        通过curl命令检查region的peer分布情况。
        
        :param op_plan: 失败的OpPlan对象
        :param region_id: region ID
        """
        thread_id = threading.get_ident()  # 获取当前线程 ID
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
            
            print(f"[Thread-{thread_id}] {region}")

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
                print(f"[Thread-{thread_id}] Target store {target_store_id} is already the leader, skipping.")
            elif target_store_id in secondary_store_ids and any(
                    peer["store_id"] == target_store_id and peer.get("role_name") == "Learner" 
                    for peer in peers
                ):
                # 检查目标store是否已经存在peer，但仍然是Learner
                print(f"[Thread-{thread_id}] Target store {target_store_id} is still Learner, pending and retry.")
                op_plan.next_retry_time = time.time() + self.retry_interval  # 设置重试时间
                op_plan.retry_count = op_plan.retry_count + 1  # 增加重试次数
                self.op_plans.put(op_plan)  # 添加到重试队列
            else:
                print(f"[Thread-{thread_id}] Target store {target_store_id} is not the leader, re-generating op_plan.")
                
                # 重新生成op_plan并添加到重试队列
                new_op_plan = self.generate_op_plan(region_id, leader_store_id, secondary_store_ids, target_store_id, op_plan.subplan_index)

                new_op_plan.next_retry_time = time.time() + self.retry_interval  # 设置重试时间
                new_op_plan.retry_count = op_plan.retry_count + 1  # 增加重试次数
                self.op_plans.put(new_op_plan)  # 添加到重试队列
        
        except subprocess.CalledProcessError as e:
            # 处理curl命令执行失败的情况
            print(f"[Thread-{thread_id}] Failed to fetch region info from PD: {e.stderr} cmd: {pd_url}")
        except json.JSONDecodeError as e:
            # 处理JSON解析错误
            print(f"[Thread-{thread_id}] Failed to parse JSON response for region {region_id}: {e}")
        except Exception as e:
            # 处理其他异常
            print(f"[Thread-{thread_id}] Error checking region peers: {e}")


    def set_round_robin(self, mock):
        """
        将分区按照 round-robin 方式分散到各个 store。
        目标 store_ids 从 self.route 中获取。
        """
        # 获取所有 store_ids
        store_ids = list(self.route.get_all_store_ids())
        if not store_ids:
            print("No store_ids found in route.")
            return
        
        # 获取所有虚拟区域 ID
        virtual_region_ids = list(self.route.virtual_region_id_map.keys())
        num_stores = len(store_ids)
        
        op_plans = []
        
        for idx, virtual_region_id in enumerate(virtual_region_ids):
            actual_region_id = self.route.virtual_region_id_map[virtual_region_id]
            current_leader_store_id = self.route.get_region_primary_store_id(virtual_region_id)
            
            # 按 round-robin 分配目标 store_id
            target_store_id = store_ids[idx % num_stores]
            
            # 如果当前 leader 已经是目标 store，则跳过
            if current_leader_store_id == target_store_id:
                continue
            
            # 获取从节点 Store ID 列表
            secondary_store_ids = self.route.get_region_secondary_store_id(virtual_region_id)
            
            # 生成操作计划
            op_plan = self.generate_op_plan(
                actual_region_id,
                current_leader_store_id,
                secondary_store_ids,
                target_store_id,
                idx  # 使用索引作为 op_index
            )
            
            if not op_plan.is_empty():
                op_plans.append(op_plan)
        
        # 执行操作计划
        self.do_operator_plan(op_plans, mock)


    def do_operator_plan(self, op_plans, mock=False):
        """
        发送operator计划到PD，并记录和打印每次请求的延迟。
        使用多线程并发处理操作计划。
        
        :param op_plans: 包含所有OpPlan对象的列表
        :param mock: 如果为True，只打印请求，不实际发送
        """
        self.mock = mock
        for op_plan in op_plans:
            self.op_plans.put(op_plan)  # 将操作计划放入线程安全的队列
        
        # 初始化原子计数器
        with self.counter_lock:
            self.atomic_counter = len(op_plans)
        
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:  # 创建线程池，最大线程数为10
            while self.atomic_counter > 0:  # 使用原子计数器作为退出条件
                if not self.op_plans.empty():
                    op_plan = self.op_plans.get()  # 从队列中获取操作计划
                    executor.submit(self.process_op_plan, op_plan)  # 提交任务到线程池
                else:
                    time.sleep(0.1)  # 如果队列为空，稍作等待