INSERT INTO curated.stroke_model_ready (
    id,
    gender,
    age,
    hypertension,
    heart_disease,
    "Residence_type",
    avg_glucose_level,
    bmi,
    smoking_status,
    stroke,
    gender_score_risk,
    age_score_risk,
    glucose_score_risk,
    bmi_score_risk,
    smoking_score_risk,
    residence_score_risk,
    "VAAF",
    "TGI"
)
VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s
)
ON CONFLICT (id)
DO UPDATE SET
    gender = EXCLUDED.gender,
    age = EXCLUDED.age,
    hypertension = EXCLUDED.hypertension,
    heart_disease = EXCLUDED.heart_disease,
    "Residence_type" = EXCLUDED."Residence_type",
    avg_glucose_level = EXCLUDED.avg_glucose_level,
    bmi = EXCLUDED.bmi,
    smoking_status = EXCLUDED.smoking_status,
    stroke = EXCLUDED.stroke,
    gender_score_risk = EXCLUDED.gender_score_risk,
    age_score_risk = EXCLUDED.age_score_risk,
    glucose_score_risk = EXCLUDED.glucose_score_risk,
    bmi_score_risk = EXCLUDED.bmi_score_risk,
    smoking_score_risk = EXCLUDED.smoking_score_risk,
    residence_score_risk = EXCLUDED.residence_score_risk,
    "VAAF" = EXCLUDED."VAAF",
    "TGI" = EXCLUDED."TGI";