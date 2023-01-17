/*
  ==========================================
  SMS to FTA Overall CvR 7 Day CVR
  ==========================================
  */
with full_acquisition_data as (
select
-- Cohort
   date_trunc('week', sms_confirmed_mx) as cohort,
-- Case for users with sms_confirmed
   case when sms_confirmed_mx is not null then 1 else 0 end as sms_confirmed,
-- Case for users who had done fta in less than 8 days after del
   case when datediff(day, sms_confirmed_mx::date, least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx)::date) < 8 then 1 else 0 end as fta
from klar.cck_funnel
where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
)
-- Aggregation for weeks
select cohort::date,
       dateadd(days, 7, cohort)::date as report_week,
       (round((sum(fta)*1.0/sum(sms_confirmed)),4))::decimal(10,4) as sms_fta_cvr,
       sum(fta) as ftas,
       sum(sms_confirmed) as sms_s
from full_acquisition_data
where cohort >= dateadd(weeks,-10,current_date)
group by cohort
order by cohort;
/*
 ==========================================
 SMS to FTA (Credit vs Debit) CvR 7 Day CVR
 ==========================================
 */
with full_acquisition_data as (
select
    -- user id
    user_id,
    -- Cohort
    date_trunc('week', sms_confirmed_mx) as cohort,
    -- Case for users with sms_confirmed
    case when sms_confirmed_mx is not null then 1 else 0 end as sms_confirmed,
    -- Case for users who had done fta in less than 8 days after del
    case when datediff(day, sms_confirmed_mx::date, least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx)::date) < 8 then 1 else 0 end as fta,
    case when (first_cck_line_timestamp_mx notnull
        and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
        and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
   from klar.cck_funnel
   where sms_confirmed_mx between '2022-10-17'::date and dateadd(day, -7, current_date)
),
credit_1t_conversion as (
    select
        cohort::date,
        dateadd(days, 7, cohort)::date as report_week,
        -- SMS Credit to FTA Cvr
        sum(fta) * 1.0 / sum(sms_confirmed) as cvr_sms_1t_credit,
        sum(sms_confirmed)                  as cck_sms_confirmed_accounts,
        sum(fta)                            as cck_first_active_account
    from full_acquisition_data
    where credit_sms = 1
    group by cohort
),
debit_1t_conversion as (
     select
         cohort::date,
         dateadd(days, 7, cohort)::date as report_week,
         -- SMS Debit to FTA Cvr
         sum(fta) * 1.0 / sum(sms_confirmed) as cvr_sms_1t_debit,
         sum(sms_confirmed)                  as debit_sms_confirmed_accounts,
         sum(fta)                            as debit_first_active_account
     from full_acquisition_data
     where credit_sms = 0
     group by cohort
)
select
    c.cohort,
    c.report_week,
    (round(c.cvr_sms_1t_credit,4))::decimal(10,4) as cvr_credit,
    c.cck_sms_confirmed_accounts,
    c.cck_first_active_account,
    (round(d.cvr_sms_1t_debit,4))::decimal(10,4) as cvr_debit,
    d.debit_sms_confirmed_accounts,
    d.debit_first_active_account
from credit_1t_conversion as c
join debit_1t_conversion as d on c.cohort = d.cohort
where c.cohort >= dateadd(weeks,-10,current_date)
order by c.cohort;
/*
 ==========================================
 SMS to Bureau CvR 7 Day CVR
 ==========================================
 */
 with full_acquisition_data as (
select
-- Cohort
   date_trunc('week', sms_confirmed_mx) as cohort,
-- bureau timestamp
   b.created_time as bureau_time,
-- Case for users with sms_confirmed
   case when sms_confirmed_mx is not null then 1 else 0 end as sms_confirmed,
-- Case for users who had done fta in less than 8 days after del
   case when datediff(day, sms_confirmed_mx::date, bureau_time::date) < 8 then 1 else 0 end as bureau_check
from klar.cck_funnel dfv
left join is_credit_bureau_circulo.rcc_ficoscore_pld_scores as b on b.user_id = dfv.user_id
where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
)
-- Aggregation for weeks
select cohort::date,
       dateadd(days, 7, cohort)::date as report_week,
       (round((sum(bureau_check)*1.0/sum(sms_confirmed)),4))::decimal(10,4) as sms_bureau_cvr,
       sum(bureau_check) as bureau_checks,
       sum(sms_confirmed) as sms_s
from full_acquisition_data
where cohort >= dateadd(weeks,-10,current_date)
group by cohort
order by cohort;
/*
 ==========================================
 SMS to Toggle on CvR 7 Day CVR
 ==========================================
 */
 with full_acquisition_data as (
select
-- Cohort
   date_trunc('week', sms_confirmed_mx) as cohort,
-- FTA
   least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx) as fta,
-- Case for users with sms_confirmed
   case when sms_confirmed_mx is not null then 1 else 0 end as sms_confirmed,
-- Case for users with first cck toggle on
   case when first_cck_toggle_on_timestamp_mx is not null then 1 else 0 end as toggle_on,
-- Case for users who had done toggle on in less than 8 days after del
   case when datediff(day, sms_confirmed_mx::date, first_cck_toggle_on_timestamp_mx::date) < 8 then 1 else 0 end as toggle_on_cvr,
   -- Case for users who had done toggle on in less than 8 days after del
   case when datediff(day, first_cck_toggle_on_timestamp_mx::date, fta::date) < 8 then 1 else 0 end as fta_cvr,
-- Credit SMS Flag
   case when (first_cck_line_timestamp_mx notnull
        and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
        and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
from klar.cck_funnel dfv
where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
and credit_sms = 1
)
-- Aggregation for weeks
select cohort::date,
       dateadd(days, 7, cohort)::date as report_week,
       (round((sum(toggle_on_cvr)*1.0/sum(sms_confirmed)),4))::decimal(10,4) as sms_toggle_on_cvr,
       sum(toggle_on_cvr) as toggles_on_converted,
       sum(sms_confirmed) as sms_s,
       (round((sum(fta_cvr)*1.0/sum(toggle_on)),4))::decimal(10,4) as toggle_on_fta_cvr,
       sum(fta_cvr) as ftas,
       sum(toggle_on) as toggles_on_universe
from full_acquisition_data
where cohort >= dateadd(weeks,-10,current_date)
group by cohort
order by cohort;
/*
 ==========================================
 Toggle on to FTA on CvR 7 Day CVR
 ==========================================
 */
 with full_acquisition_data as (
select
-- Cohort
   date_trunc('week', first_cck_toggle_on_timestamp_mx) as cohort,
-- FTA
   least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx) as fta,
-- Case for users with sms_confirmed
   case when first_cck_toggle_on_timestamp_mx is not null then 1 else 0 end as toggle_on,
-- Case for users who had done toggle on in less than 8 days after del
   case when datediff(day, first_cck_toggle_on_timestamp_mx::date, fta::date) < 8 then 1 else 0 end as fta_cvr,
-- Credit SMS Flag
   case when (first_cck_line_timestamp_mx notnull
        and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
        and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
from klar.cck_funnel dfv
where first_cck_toggle_on_timestamp_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
and credit_sms = 1
)
-- Aggregation for weeks
select cohort::date,
       dateadd(days, 7, cohort)::date as report_week,
       (round((sum(fta_cvr)*1.0/sum(toggle_on)),4))::decimal(10,4) as toggle_on_fta_cvr,
       sum(fta_cvr) as ftas,
       sum(toggle_on) as toggle_ons
from full_acquisition_data
where cohort >= dateadd(weeks,-10,current_date)
group by cohort
order by cohort;
/*
 ==========================================
 CCK Line to Toggle On CvR 7 Day CVR deep look into cohort week 2022-11-28
 ==========================================
 */
 with full_acquisition_data as (
select
-- Cohort
   date_trunc('week', first_cck_line_timestamp_mx) as cohort,
-- Origination campaign
   dfv.origination_campaign,
-- Case for users with sms_confirmed
   case when first_cck_line_timestamp_mx is not null then 1 else 0 end as lines_offered,
-- Case for users who had done toggle on in less than 8 days after del
   case when datediff(day, first_cck_line_timestamp_mx::date,first_cck_toggle_on_timestamp_mx::date) < 8 then 1 else 0 end as line_activation,
-- Tier A user flag
   (kyc_tier.current = 'TIER_A')::int as tier_a,
-- KYC Status
   (onboarding_data.current_state in ('DEBIT_ONLY_USER', 'NO_DEPOSIT')):: int as debit_state,
   (onboarding_data.current_state = 'KYC_VERIFICATION_FAILED')::int as verification_failure,
   (onboarding_data.current_state = 'KYC_VERIFICATION_RETRY')::int as verification_retry,
   (onboarding_data.current_state = 'KYC_VERIFICATION_REQUIRED')::int as verification_required,
   (onboarding_data.current_state = 'KYC_VERIFICATION_IN_PROGRESS')::int as verification_progress,
-- Credit SMS Flag
   case when (first_cck_line_timestamp_mx notnull
        and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
        and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
from klar.cck_funnel dfv
left join is_kyc.kyc_tier
    on dfv.user_id = kyc_tier.user_id
left join is_onboarding_service.onboarding_data
    on dfv.user_id = onboarding_data.user_id
where first_cck_line_timestamp_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
and first_cck_line_timestamp_mx notnull
and migrated_user = 1
)
-- Aggregation for weeks
select cohort::date,
       dateadd(days, 7, cohort)::date as report_week,
       (round(avg(tier_a::float),4))::decimal(10,4) as tier_a_prop,
       (round(avg(debit_state::float),4))::decimal(10,4) as kyc_debit,
       (round(avg(verification_failure::float),4))::decimal(10,4) as kyc_failure,
       (round(avg(verification_retry::float),4))::decimal(10,4) as kyc_retry,
       (round(avg(verification_required::float),4))::decimal(10,4) as kyc_required,
       (round(avg(verification_progress::float),4))::decimal(10,4) as kyc_in_progress,
       (round((sum(line_activation)*1.0/sum(lines_offered)),4))::decimal(10,4) as toggle_on_fta_cvr,
       sum(line_activation) as toggles_on,
       sum(lines_offered) as cck_lines
from full_acquisition_data
where cohort >= dateadd(weeks,-10,current_date)
group by cohort
order by cohort;
/* ==========================================
 SMS to FTA CvR 7 Day CVR by origination campaign
 ==========================================
 */
 select * from (
    with full_acquisition_data as (
        select
        -- Cohort
           date_trunc('week', sms_confirmed_mx) as cohort,
        -- Origination Campaign
           origination_campaign,
        -- FTA
           least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx) as fta,
        -- Case for users with sms_confirmed
           case when sms_confirmed_mx is not null then 1 else 0 end as sms_confirmed,
        -- Case for users who had done fta in less than 8 days after sms
           case when datediff(day, sms_confirmed_mx::date, fta::date) < 8 then 1 else 0 end as sms_fta_cvr,
        -- Credit SMS Flag
           case when (first_cck_line_timestamp_mx notnull
                and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
                and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
        from klar.cck_funnel dfv
        where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
        and credit_sms = 1
        )
    select cohort,
           origination_campaign,
           sum(sms_fta_cvr) * 1.0 / sum(sms_confirmed) as conv_sms_fta
    from full_acquisition_data
    group by cohort, origination_campaign
    having cohort >= dateadd(weeks,-10,current_date)
    order by cohort
    )
PIVOT(
    sum(conv_sms_fta) for origination_campaign in (
        'RCL Alpha Baseline',
        'RCL Alpha Baseline (10)',
        'RCL Alpha Baseline (14)',
        'RCL Alpha Upsize 1',
        'RCL Alpha Upsize 2',
        'RCL Alpha Upsize 2 (10)',
        'RCL Alpha Upsize 2 (14)',
        'RCL Foundational Test',
        'RCL Foundational Test 0.5K',
        'RCL Foundational Test 0.5K (10)',
        'RCL Foundational Test 0.5K (14)',
        'RCL Foundational Test 1K',
        'RCL Foundational Test 1K (10)',
        'RCL Foundational Test 1K (14)',
        'RCL Foundational Test 2K',
        'RCL Foundational Test 2K (10)',
        'RCL Foundational Test 2K (14)',
        'RCL Foundational Test 3K',
        'RCL Foundational Test 3K (10)',
        'RCL Foundational Test 3K (14)'
    )
         )
order by cohort;
/*
 ==========================================
 SMS to Toggle ON CvR 7 Day CVR by origination campaign
 ==========================================
 */
 select * from (
    with full_acquisition_data as (
        select
        -- Cohort
           date_trunc('week', sms_confirmed_mx) as cohort,
        -- Origination Campaign
           origination_campaign,
        -- Case for users with sms_confirmed
           case when sms_confirmed_mx is not null then 1 else 0 end as sms_confirmed,
        -- Case for users who had done toggle on in less than 8 days after del
           case when datediff(day, sms_confirmed_mx::date, first_cck_toggle_on_timestamp_mx::date) < 8 then 1 else 0 end as toggle_on_cvr,
        -- Credit SMS Flag
           case when (first_cck_line_timestamp_mx notnull
                and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
                and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
        from klar.cck_funnel dfv
        where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
        and credit_sms = 1
        )
    select cohort,
           origination_campaign,
           sum(toggle_on_cvr) * 1.0 / sum(sms_confirmed) as conv_sms_toggle_on
    from full_acquisition_data
    group by cohort, origination_campaign
    having cohort >= dateadd(weeks,-10,current_date)
    order by cohort
    )
PIVOT(
    sum(conv_sms_toggle_on) for origination_campaign in (
        'RCL Alpha Baseline',
        'RCL Alpha Baseline (10)',
        'RCL Alpha Baseline (14)',
        'RCL Alpha Upsize 1',
        'RCL Alpha Upsize 2',
        'RCL Alpha Upsize 2 (10)',
        'RCL Alpha Upsize 2 (14)',
        'RCL Foundational Test',
        'RCL Foundational Test 0.5K',
        'RCL Foundational Test 0.5K (10)',
        'RCL Foundational Test 0.5K (14)',
        'RCL Foundational Test 1K',
        'RCL Foundational Test 1K (10)',
        'RCL Foundational Test 1K (14)',
        'RCL Foundational Test 2K',
        'RCL Foundational Test 2K (10)',
        'RCL Foundational Test 2K (14)',
        'RCL Foundational Test 3K',
        'RCL Foundational Test 3K (10)',
        'RCL Foundational Test 3K (14)'
    )
         )
order by cohort;
/*
 ==========================================
 Toggle on to FTA CvR 7 Day CVR by origination campaign
 ==========================================
 */
 select * from (
    with full_acquisition_data as (
        select
        -- Cohort
           date_trunc('week', sms_confirmed_mx) as cohort,
        -- Origination Campaign
           origination_campaign,
        -- FTA
           least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx) as fta,
        -- Case for users with first cck toggle on
           case when first_cck_toggle_on_timestamp_mx is not null then 1 else 0 end as toggle_on,
        -- Case for users who had done toggle on in less than 8 days after del
           case when datediff(day, first_cck_toggle_on_timestamp_mx::date, fta::date) < 8 then 1 else 0 end as fta_cvr,
        -- Credit SMS Flag
           case when (first_cck_line_timestamp_mx notnull
                and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
                and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
        from klar.cck_funnel dfv
        where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
        and credit_sms = 1
        )
    select cohort,
           origination_campaign,
           case when sum(toggle_on) = 0 then null else (round(sum(fta_cvr)*1.0/sum(toggle_on),4))::decimal(10,4) end as conv_toggle_on_to_fta
    from full_acquisition_data
    group by cohort, origination_campaign
    having cohort >= dateadd(weeks,-10,current_date)
    order by cohort
    )
PIVOT(
    sum(conv_toggle_on_to_fta) for origination_campaign in (
        'RCL Alpha Baseline',
        'RCL Alpha Baseline (10)',
        'RCL Alpha Baseline (14)',
        'RCL Alpha Upsize 1',
        'RCL Alpha Upsize 2',
        'RCL Alpha Upsize 2 (10)',
        'RCL Alpha Upsize 2 (14)',
        'RCL Foundational Test',
        'RCL Foundational Test 0.5K',
        'RCL Foundational Test 0.5K (10)',
        'RCL Foundational Test 0.5K (14)',
        'RCL Foundational Test 1K',
        'RCL Foundational Test 1K (10)',
        'RCL Foundational Test 1K (14)',
        'RCL Foundational Test 2K',
        'RCL Foundational Test 2K (10)',
        'RCL Foundational Test 2K (14)',
        'RCL Foundational Test 3K',
        'RCL Foundational Test 3K (10)',
        'RCL Foundational Test 3K (14)'
    )
         )
order by cohort;
/*
 ==========================================
 CCK Line to Toggle On CvR 7 Day CVR by origination campaign
 ==========================================
 */
 select * from (
    with full_acquisition_data as (
            select
            -- Cohort
               date_trunc('week', first_cck_line_timestamp_mx) as cohort,
            -- Origination Campaign
               origination_campaign,
            -- Users with CCK Line
                case when first_cck_line_timestamp_mx notnull then 1 else 0 end as line_available,
            --flag for offer after 2 days
                case when datediff(day, first_cck_line_timestamp_mx::date, first_cck_toggle_on_timestamp_mx::date) < 8 then 1 else 0 end as line_activation_cvr,
            -- Credit SMS Flag
               case when (first_cck_line_timestamp_mx notnull
                    and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
                    and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
            from klar.cck_funnel dfv
            where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
            and credit_sms = 1
            )
        select cohort,
               origination_campaign,
               case when sum(line_available) = 0 then null else (round(sum(line_activation_cvr)*1.0/sum(line_available),4))::decimal(10,4) end as conv_line_toggle_on
        from full_acquisition_data
        group by cohort, origination_campaign
        having cohort >= dateadd(weeks,-10,current_date)
        order by cohort
    )
PIVOT(
    sum(conv_line_toggle_on) for origination_campaign in (
        'RCL Alpha Baseline',
        'RCL Alpha Baseline (10)',
        'RCL Alpha Baseline (14)',
        'RCL Alpha Upsize 1',
        'RCL Alpha Upsize 2',
        'RCL Alpha Upsize 2 (10)',
        'RCL Alpha Upsize 2 (14)',
        'RCL Foundational Test',
        'RCL Foundational Test 0.5K',
        'RCL Foundational Test 0.5K (10)',
        'RCL Foundational Test 0.5K (14)',
        'RCL Foundational Test 1K',
        'RCL Foundational Test 1K (10)',
        'RCL Foundational Test 1K (14)',
        'RCL Foundational Test 2K',
        'RCL Foundational Test 2K (10)',
        'RCL Foundational Test 2K (14)',
        'RCL Foundational Test 3K',
        'RCL Foundational Test 3K (10)',
        'RCL Foundational Test 3K (14)'
    )
         )
order by cohort;
/*
 ==========================================
 FTA Blend Overall Percentage
 ==========================================
 */
with fta_blend as (
    select
    -- Cohort
        date_trunc('week', sms_confirmed_mx) as cohort,
    -- Origination Campaign
        origination_campaign,
    -- FTA
        least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx) as credit_first_transaction,
    -- FTA Reason
        case
            when (credit_first_purchase_timestamp_mx notnull and credit_first_purchase_timestamp_mx = credit_first_transaction) then 'Credit_Purchase'
            when (first_deposit_mx notnull and first_deposit_mx = credit_first_transaction) then 'Deposit'
            when (first_cash_installment_loan_timestamp_mx notnull and first_cash_installment_loan_timestamp_mx = credit_first_transaction) then 'Cashloan'
            end as fta_reason,
    -- FTA Reasons
    -- Purchases
        (fta_reason = 'Credit_Purchase')::int as credit_purchases,
    -- Deposits
        (fta_reason = 'Deposit')::int as deposits,
    -- Cashloans
        (fta_reason = 'Cashloan')::int as cashloans,
    -- Credit SMS Flag
        case
        when (first_cck_line_timestamp_mx notnull
            and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
            and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
    from klar.cck_funnel cf
    where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
        and credit_sms = 1
)
select
    cohort::date,
    dateadd(days, 7, cohort)::date as report_week,
    (round(avg(credit_purchases::float),4))::decimal(10,4) as avg_purchases,
    (round(avg(deposits::float) ,4))::decimal(10,4) as avg_deposits,
    (round(avg(cashloans::float) ,4))::decimal(10,4) as avg_cashloans
from fta_blend
-- where origination_campaign = 'RCL Alpha Upsize 2'
-- group by cohort, origination_campaign
group by cohort
order by cohort;
/*
 ==========================================
 FTA Blend Overall
 ==========================================
 */
with fta_blend as (
    select
    -- Cohort
        date_trunc('week', sms_confirmed_mx) as cohort,
    -- Origination Campaign
        origination_campaign,
    -- FTA
        least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx) as credit_first_transaction,
    -- FTA Reason
        case
            when (credit_first_purchase_timestamp_mx notnull and credit_first_purchase_timestamp_mx = credit_first_transaction) then 'Credit_Purchase'
            when (first_deposit_mx notnull and first_deposit_mx = credit_first_transaction) then 'Deposit'
            when (first_cash_installment_loan_timestamp_mx notnull and first_cash_installment_loan_timestamp_mx = credit_first_transaction) then 'Cashloan'
            end as fta_reason,
    -- FTA Reasons
    -- Purchases
        (fta_reason = 'Credit_Purchase')::int as credit_purchases,
    -- Deposits
        (fta_reason = 'Deposit')::int as deposits,
    -- Cashloans
        (fta_reason = 'Cashloan')::int as cashloans,
    -- Credit SMS Flag
        case
        when (first_cck_line_timestamp_mx notnull
            and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
            and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
    from klar.cck_funnel cf
    where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
        and credit_sms = 1
)
select
    cohort::date,
    dateadd(days, 7, cohort)::date as report_week,
    (round(sum(credit_purchases::float),4))::decimal(10,4) as sum_purchases,
    (round(sum(deposits::float) ,4))::decimal(10,4) as sum_deposits,
    (round(sum(cashloans::float) ,4))::decimal(10,4) as sum_cashloans
from fta_blend
-- where origination_campaign = 'RCL Alpha Upsize 2'
-- group by cohort, origination_campaign
group by cohort
order by cohort;
/*
 ==========================================
 KYC Information by SMS Confirmed
 ==========================================
 */
with full_acquisition_data as (
select
-- Cohort
   date_trunc('week', sms_confirmed_mx) as cohort,
-- Origination campaign
   dfv.origination_campaign,
-- Case for users with sms_confirmed
   case when dfv.sms_confirmed_mx is not null then 1 else 0 end as sms_confirmed,
-- Tier A user flag
   (kyc_tier.current = 'TIER_A')::int as tier_a,
-- KYC Status
   (onboarding_data.current_state in ('DEBIT_ONLY_USER', 'NO_DEPOSIT')):: int as debit_state,
   (onboarding_data.current_state = 'KYC_VERIFICATION_FAILED')::int as verification_failure,
   (onboarding_data.current_state = 'KYC_VERIFICATION_RETRY')::int as verification_retry,
   (onboarding_data.current_state = 'KYC_VERIFICATION_REQUIRED')::int as verification_required,
   (onboarding_data.current_state = 'KYC_VERIFICATION_IN_PROGRESS')::int as verification_progress,
-- Credit SMS Flag
   case when (first_cck_line_timestamp_mx notnull
        and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
        and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
from klar.cck_funnel dfv
left join is_kyc.kyc_tier
    on dfv.user_id = kyc_tier.user_id
left join is_onboarding_service.onboarding_data
    on dfv.user_id = onboarding_data.user_id
where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
and first_cck_line_timestamp_mx notnull
and migrated_user = 0
)
-- Aggregation for weeks
select cohort::date,
       dateadd(days, 7, cohort)::date as report_week,
       (round(avg(tier_a::float),4))::decimal(10,4) as tier_a_prop,
       (round(avg(debit_state::float),4))::decimal(10,4) as kyc_debit,
       (round(avg(verification_failure::float),4))::decimal(10,4) as kyc_failure,
       (round(avg(verification_retry::float),4))::decimal(10,4) as kyc_retry,
       (round(avg(verification_required::float),4))::decimal(10,4) as kyc_required,
       (round(avg(verification_progress::float),4))::decimal(10,4) as kyc_in_progress,
       sum(sms_confirmed) as sms_credit
from full_acquisition_data
group by cohort
order by cohort;
/*
 ==========================================
 KYC Information by CCK Line Timestamp
 ==========================================
 */
with full_acquisition_data as (
select
-- Cohort
   date_trunc('week', first_cck_line_timestamp_mx) as cohort,
-- Origination campaign
   dfv.origination_campaign,
-- Case for users with sms_confirmed
   case when dfv.sms_confirmed_mx is not null then 1 else 0 end as sms_confirmed,
-- Tier A user flag
   (kyc_tier.current = 'TIER_A')::int as tier_a,
-- KYC Status
   (onboarding_data.current_state in ('DEBIT_ONLY_USER', 'NO_DEPOSIT')):: int as debit_state,
   (onboarding_data.current_state = 'KYC_VERIFICATION_FAILED')::int as verification_failure,
   (onboarding_data.current_state = 'KYC_VERIFICATION_RETRY')::int as verification_retry,
   (onboarding_data.current_state = 'KYC_VERIFICATION_REQUIRED')::int as verification_required,
   (onboarding_data.current_state = 'KYC_VERIFICATION_IN_PROGRESS')::int as verification_progress,
-- Credit SMS Flag
   case when (first_cck_line_timestamp_mx notnull
        and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
        and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
from klar.cck_funnel dfv
left join is_kyc.kyc_tier
    on dfv.user_id = kyc_tier.user_id
left join is_onboarding_service.onboarding_data
    on dfv.user_id = onboarding_data.user_id
where first_cck_line_timestamp_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
and first_cck_line_timestamp_mx notnull
and migrated_user = 0
)
-- Aggregation for weeks
select cohort::date,
       dateadd(days, 7, cohort)::date as report_week,
       (round(avg(tier_a::float),4))::decimal(10,4) as tier_a_prop,
       (round(avg(debit_state::float),4))::decimal(10,4) as kyc_debit,
       (round(avg(verification_failure::float),4))::decimal(10,4) as kyc_failure,
       (round(avg(verification_retry::float),4))::decimal(10,4) as kyc_retry,
       (round(avg(verification_required::float),4))::decimal(10,4) as kyc_required,
       (round(avg(verification_progress::float),4))::decimal(10,4) as kyc_in_progress,
       sum(sms_confirmed) as sms_credit
from full_acquisition_data
group by cohort
order by cohort;
/*
 ==========================================
 Toggle on to FTA by channel and campaign
 ==========================================
 */
 with adjust_processed as (
     select
         DISTINCT adjust.klaruserid as user_id,
                  network_name
     from public.adjust
         join (
         select
            klaruserid,
            min(last_session_time) as min_created
         from public.adjust
         group by klaruserid) as min_time_user
     on adjust.klaruserid = min_time_user.klaruserid and adjust.last_session_time = min_time_user.min_created
     where adjust.klaruserid notnull
 ),
mapped_channels as (
    select
        user_id,
        case
            when referral_code is not null then 'Referral'
            when network_name = '5fow643x' and referral_code is null then 'ZoomD'
            when network_name = 'AdAction' and referral_code is null then 'AdAction'
            when network_name = 'Apple Search Ads' and referral_code is null then 'Apple Search'
            when network_name = 'Carlos Estrada' and referral_code is null then 'Influencers'
            when network_name = 'Eduardo Rosas' and referral_code is null then 'Influencers'
            when network_name like '%Facebook%' and referral_code is null then 'Facebook'
            when network_name like '%Google%' and referral_code is null then 'Google'
            when network_name like '%Influencer%' and referral_code is null then 'Influencers'
            when network_name like 'Instagram Installs' and referral_code is null then 'Facebook'
            when network_name like 'Joel Video' and referral_code is null then 'Influencers'
            when network_name like 'Klar.mx' and referral_code is null then 'Klar'
            when network_name like '%Liftoff%' and referral_code is null then 'Liftoff'
            when network_name like 'New_Respaldo' and referral_code is null then 'Klar'
            when network_name like 'Snapchat Installs' and referral_code is null then 'Snapchat'
            when network_name like 'TikTok ZoomD' and referral_code is null then 'TikTok'
            when network_name like 'Tiktok Organic' and referral_code is null then 'TikTok'
            when network_name like 'Tiktok Installs' and referral_code is null then 'TikTok'
            when network_name like 'UA48736' and referral_code is null then 'ZoomD'
            when network_name like 'Unattributed' and referral_code is null then 'Facebook'
            when network_name like '%Untrusted Devices%' and referral_code is null then 'Digital Turbine'
            when network_name like 'tiktok' and referral_code is null then 'TikTok'
            when network_name like '%ZoomD%' and referral_code is null then 'ZoomD'
            when network_name like '%Zoomd%' and referral_code is null then 'ZoomD'
            when network_name like '%Twitter%' and referral_code is null then 'Twitter'
            when network_name = 'Referral' and referral_code is null then 'Referral'
            when network_name = 'Taboola' and referral_code is null then 'Taboola'
            when network_name = 'Oppizi' and referral_code is null then 'Oppizi'
            when network_name = 'Offline' and referral_code is null then 'Offline'
            when network_name = 'Lychee' and referral_code is null then 'Lychee'
            when network_name = 'Destacame' and referral_code is null then 'Destacame'
            when network_name = 'MiQ' and referral_code is null then 'MiQ'
            when network_name = 'Leadgenios' and referral_code is null then 'Leadgenios'
            when network_name = 'Mapendo' and referral_code is null then 'Mapendo'
            when network_name like '%Organic%' and referral_code is null then 'Organic'
            when network_name is null and referral_code is null then 'Organic'
            else 'Not_Mapped'
            end as channel,
        referral_code,
        network_name
    from adjust_processed as adj
    left join growth.referral_info as referral on adj.user_id = referree_user_id
),
 full_acquisition_data as (
        select
        -- Cohort
           date_trunc('week', sms_confirmed_mx) as cohort,
        -- Channel of adquisiction
           coalesce(channel,'Organic') as channel_adq,
        -- Presented channel
           case when channel_adq not in ('Organic', 'Facebook', 'Google', 'Referral', 'Apple Search', 'TikTok', 'ZoomD', 'Twitter') then 'Affiliation'
               else channel_adq end as presented_channel,
        -- Origination Campaign
           origination_campaign,
        -- FTA
           least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx) as fta,
        -- Case for users with first cck toggle on
           case when first_cck_toggle_on_timestamp_mx is not null then 1 else 0 end as toggle_on,
        -- Case for users who had done toggle on in less than 8 days after del
           case when datediff(day, first_cck_toggle_on_timestamp_mx::date, fta::date) < 8 then 1 else 0 end as fta_cvr,
        -- Credit SMS Flag
           case when (first_cck_line_timestamp_mx notnull
                and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
                and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
        from klar.cck_funnel dfv
        left join mapped_channels as channel on channel.user_id = dfv.user_id
        where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
        and credit_sms = 1
        )
    select cohort,
           origination_campaign,
           presented_channel,
           sum(fta_cvr) as ftas_converted,
           sum(toggle_on) as toggles_on,
           case when toggles_on = 0 then null else (round(ftas_converted*1.0/toggles_on,4))::decimal(10,4) end as conv_toggle_on_to_fta,
           (toggles_on*1.0 / (SUM(sum(toggle_on)) OVER ()))::decimal(10,4) AS Percentage_toggle_on
    from full_acquisition_data
    group by cohort, presented_channel, origination_campaign
    having cohort >= dateadd(weeks,-10,current_date)
    order by cohort, presented_channel , origination_campaign;
/*
 ==========================================
 Toggle on to FTA by channel and campaign
 ==========================================
 */
 with adjust_processed as (
     select
         DISTINCT adjust.klaruserid as user_id,
                  network_name
     from public.adjust
         join (
         select
            klaruserid,
            min(last_session_time) as min_created
         from public.adjust
         group by klaruserid) as min_time_user
     on adjust.klaruserid = min_time_user.klaruserid and adjust.last_session_time = min_time_user.min_created
     where adjust.klaruserid notnull
 ),
mapped_channels as (
    select
        user_id,
        case
            when referral_code is not null then 'Referral'
            when network_name = '5fow643x' and referral_code is null then 'ZoomD'
            when network_name = 'AdAction' and referral_code is null then 'AdAction'
            when network_name = 'Apple Search Ads' and referral_code is null then 'Apple Search'
            when network_name = 'Carlos Estrada' and referral_code is null then 'Influencers'
            when network_name = 'Eduardo Rosas' and referral_code is null then 'Influencers'
            when network_name like '%Facebook%' and referral_code is null then 'Facebook'
            when network_name like '%Google%' and referral_code is null then 'Google'
            when network_name like '%Influencer%' and referral_code is null then 'Influencers'
            when network_name like 'Instagram Installs' and referral_code is null then 'Facebook'
            when network_name like 'Joel Video' and referral_code is null then 'Influencers'
            when network_name like 'Klar.mx' and referral_code is null then 'Klar'
            when network_name like '%Liftoff%' and referral_code is null then 'Liftoff'
            when network_name like 'New_Respaldo' and referral_code is null then 'Klar'
            when network_name like 'Snapchat Installs' and referral_code is null then 'Snapchat'
            when network_name like 'TikTok ZoomD' and referral_code is null then 'TikTok'
            when network_name like 'Tiktok Organic' and referral_code is null then 'TikTok'
            when network_name like 'Tiktok Installs' and referral_code is null then 'TikTok'
            when network_name like 'UA48736' and referral_code is null then 'ZoomD'
            when network_name like 'Unattributed' and referral_code is null then 'Facebook'
            when network_name like '%Untrusted Devices%' and referral_code is null then 'Digital Turbine'
            when network_name like 'tiktok' and referral_code is null then 'TikTok'
            when network_name like '%ZoomD%' and referral_code is null then 'ZoomD'
            when network_name like '%Zoomd%' and referral_code is null then 'ZoomD'
            when network_name like '%Twitter%' and referral_code is null then 'Twitter'
            when network_name = 'Referral' and referral_code is null then 'Referral'
            when network_name = 'Taboola' and referral_code is null then 'Taboola'
            when network_name = 'Oppizi' and referral_code is null then 'Oppizi'
            when network_name = 'Offline' and referral_code is null then 'Offline'
            when network_name = 'Lychee' and referral_code is null then 'Lychee'
            when network_name = 'Destacame' and referral_code is null then 'Destacame'
            when network_name = 'MiQ' and referral_code is null then 'MiQ'
            when network_name = 'Leadgenios' and referral_code is null then 'Leadgenios'
            when network_name = 'Mapendo' and referral_code is null then 'Mapendo'
            when network_name like '%Organic%' and referral_code is null then 'Organic'
            when network_name is null and referral_code is null then 'Organic'
            else 'Not_Mapped'
            end as channel,
        referral_code,
        network_name
    from adjust_processed as adj
    left join growth.referral_info as referral on adj.user_id = referree_user_id
),
 full_acquisition_data as (
        select
        -- Cohort
           date_trunc('week', sms_confirmed_mx) as cohort,
        -- Channel of adquisiction
           coalesce(channel,'Organic') as channel_adq,
        -- Presented channel
           case when channel_adq not in ('Organic', 'Facebook', 'Google', 'Referral', 'Apple Search', 'TikTok', 'ZoomD', 'Twitter') then 'Affiliation'
               else channel_adq end as presented_channel,
        -- FTA
           least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx) as fta,
        -- Case for users with first cck toggle on
           case when first_cck_toggle_on_timestamp_mx is not null then 1 else 0 end as toggle_on,
        -- Case for users who had done toggle on in less than 8 days after del
           case when datediff(day, first_cck_toggle_on_timestamp_mx::date, fta::date) < 8 then 1 else 0 end as fta_cvr,
        -- Credit SMS Flag
           case when (first_cck_line_timestamp_mx notnull
                and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
                and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms
        from klar.cck_funnel dfv
        left join mapped_channels as channel on channel.user_id = dfv.user_id
        where sms_confirmed_mx between '2022-10-17'::date and dateadd(day,-7, current_date)
        and credit_sms = 1
        )
    select cohort,
           presented_channel,
           sum(fta_cvr) as ftas_converted,
           sum(toggle_on) as toggles_on,
           case when toggles_on = 0 then null else (round(ftas_converted*1.0/toggles_on,4))::decimal(10,4) end as conv_toggle_on_to_fta,
           (toggles_on*1.0 / (SUM(sum(toggle_on)) OVER ()))::decimal(10,4) AS Percentage_toggle_on
    from full_acquisition_data
    group by cohort, presented_channel
    having cohort >= dateadd(weeks,-10,current_date)
    order by cohort, presented_channel;
/*

