class Serializable:
    @classmethod
    def from_dict(cls, d: dict):
        return cls(**d)