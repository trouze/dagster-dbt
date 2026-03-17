CASE
  WHEN last_hygiene_date IS NULL THEN TRUE
  WHEN DATEDIFF('month', last_hygiene_date, CURRENT_DATE()) >= staleness_months THEN TRUE
  ELSE FALSE
END
