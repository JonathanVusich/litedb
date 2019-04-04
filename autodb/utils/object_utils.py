from typing import Dict


def retrieve_possible_object_indexes(complex_object: object) -> Dict[str, object]:
    """
    This method retrieves public variables of a class
    that can be used to build indexes.
    :param complex_object:
    :return:
    """

    indexes: Dict[str, object] = {}
    object_vars = vars(complex_object)
    for var in object_vars:
        if not var.startswith("_"):
            var_value = getattr(complex_object, var)
            indexes.update({var: var_value})
    return indexes
