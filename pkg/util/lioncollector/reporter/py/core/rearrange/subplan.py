from core.analyze.clump import Clump

class SubPlan:
    def __init__(self, clump, original_store_ids, target_store_id):
        self.clump = clump
        self.original_store_ids = original_store_ids
        self.target_store_id = target_store_id

    def __repr__(self):
        return f"SubPlan(clump={self.clump}, original_store_ids={self.original_store_ids}, target_store_id={self.target_store_id})"