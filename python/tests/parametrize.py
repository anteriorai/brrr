from collections.abc import Sequence


def names(base_name: str, names: Sequence[str]) -> Sequence[str]:
    return map(lambda x: f"{base_name}{x}", names)
