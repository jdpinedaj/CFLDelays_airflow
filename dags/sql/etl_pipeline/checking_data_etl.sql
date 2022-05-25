SELECT id_train,
    train_weight,
    weight_length,
    weight_wagon,
    cancelled_train_bin
FROM {{ params.table_name }}
ORDER BY id_train;