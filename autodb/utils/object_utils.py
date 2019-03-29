from typing import Dict, Union

acceptable_indexes = (int, bool, float, str, bytes, bytearray)


def retrieve_object_indexes(complex_object: object) -> Dict[str, Union[int, bool, float, str, bytes, bytearray]]:
    indexes: Dict[str, Union[int, bool, float, str, bytes, bytearray]] = {}
    object_vars = vars(complex_object)
    for var in object_vars:
        var_value = getattr(complex_object, var)
        if isinstance(var_value, acceptable_indexes):
            indexes.update({var: var_value})
    return indexes
