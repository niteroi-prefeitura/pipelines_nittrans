from registers import clear_old_registers, add_registers


def update_layers_on_arcgis(traffic_layer, df):
    errors = []

    registers_removed = clear_old_registers(traffic_layer)
    if isinstance(registers_removed, str):
        errors.append(registers_removed)

    registers_added = add_registers(df, traffic_layer)
    if isinstance(registers_added, str):
        errors.append(registers_added)

    return errors
