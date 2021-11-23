from typing import Dict, List


class Serializable:
    @classmethod
    def from_dict(cls, d: dict):
        return cls(**d)


class ToDB:
    @classmethod
    def export_order(cls) -> List[str]:
        raise NotImplementedError("export order was not implemented")

    @classmethod
    def override_target_names(cls) -> Dict[str, str]:
        return {}

    def column_mapping(self) -> Dict[str, str]:
        overrides = self.override_target_names()
        return {field: overrides.get(field, field) for field in self.export_order()}
