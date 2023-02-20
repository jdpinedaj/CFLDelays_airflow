SELECT column_name,
    ','
FROM information_schema.columns
WHERE table_schema = 'public_ready_for_ml'
    AND table_name = 'train_data'