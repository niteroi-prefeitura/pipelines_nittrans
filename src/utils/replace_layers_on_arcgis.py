from registers import clear_all_registers_on_agol_layer, add_polyline_registers_on_agol_layer


def replace_polyline_layers_on_agol(agol_layer, df, attribute_map: dict[str, str]):
    errors = []

    registers_removed = clear_all_registers_on_agol_layer(agol_layer)
    if isinstance(registers_removed, str):
        errors.append(registers_removed)

    registers_added = add_polyline_registers_on_agol_layer(
        df, attribute_map, agol_layer)
    if isinstance(registers_added, str):
        errors.append(registers_added)

    return errors
