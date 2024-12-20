from collections import defaultdict


class InMemoryIntegrationStateManager:

    def __init__(self, **kwargs):
        self.kvs = {}
        self.groups = defaultdict(set)

    async def get_state(self, integration_id: str, action_id: str, source_id: str = "no-source") -> dict:
        key = f"integration_state.{integration_id}.{action_id}.{source_id}"
        return self.kvs.get(key)

    async def set_state(self, integration_id: str, action_id: str, state: dict, source_id: str = "no-source"):
        key = f"integration_state.{integration_id}.{action_id}.{source_id}"
        self.kvs[key] = state

    async def delete_state(self, integration_id: str, action_id: str, source_id: str = "no-source"):
        key = f"integration_state.{integration_id}.{action_id}.{source_id}"
        self.kvs.pop(key, None)

    async def group_add(self, group_name: str, values: list):
        self.groups[group_name].update(values)

    async def group_get(self, group_name: str):
        return self.groups.get(group_name, set())

    async def group_move(self, from_group: str, to_group: str, values: list):
        self.groups.setdefault(to_group, set()).update(values)
        self.groups[from_group] -= set(values)
        return len(values)

    async def group_remove(self, group_name: str, values: list):
        self.groups[group_name].difference_update(values)
        return len(values)

    async def group_ismember(self, group_name: str, value: str) -> bool:
        return value in self.groups.get(group_name, set())

    def __str__(self):
        return f"{self.__class__.__name__}({self.kvs}, {self.groups})"
