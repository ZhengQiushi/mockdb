import threading

class BucketedDict:
    def __init__(self, num_buckets=1024):
        self.num_buckets = num_buckets
        self.buckets = [{} for _ in range(num_buckets)]
        self.locks = [threading.Lock() for _ in range(num_buckets)]

    def __getstate__(self):
        # 序列化时排除线程锁
        state = self.__dict__.copy()
        del state['locks']
        return state

    def __setstate__(self, state):
        # 反序列化时恢复状态并重新初始化线程锁
        self.__dict__.update(state)
        self.locks = [threading.Lock() for _ in range(self.num_buckets)]
        
    def _get_bucket_index(self, key):
        # print("_get_bucket_index: ", key, hash(key) % self.num_buckets)
        return hash(key) % self.num_buckets
    
    def get(self, key, default=None):
        bucket_index = self._get_bucket_index(key)
        with self.locks[bucket_index]:
            return self.buckets[bucket_index].get(key, default)
    
    def set(self, key, value):
        bucket_index = self._get_bucket_index(key)
        with self.locks[bucket_index]:
            self.buckets[bucket_index][key] = value
    
    def delete(self, key):
        bucket_index = self._get_bucket_index(key)
        with self.locks[bucket_index]:
            if key in self.buckets[bucket_index]:
                del self.buckets[bucket_index][key]