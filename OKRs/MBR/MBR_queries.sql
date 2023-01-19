--Funnel View;
with started_signup as (
	with started_signups as (
	 select date_trunc('month', started_web_signup_mx) as mes,
	 count(user_id) as started_signup
	 from klar.cck_funnel cf
	 where started_web_signup_mx notnull
	 and started_web_signup_mx > '2021-04-01'
	 group by mes
	 order by 1 asc
),
	not_completed_signup as (
	     select date_trunc('month', signup_created_timestamp) as day_notC,
	           count(email) as not_c_signup
	     from klar.signup_not_completed
	     where signup_created_timestamp notnull
	     group by day_notC
)
select day_notC as fecha,
           not_c_signup + started_signup as signups_started
    from not_completed_signup
             left join started_signups on not_completed_signup.day_notC = started_signups.mes
    order by fecha asc
),
	completed_signup as (
	select date_trunc('month', ended_web_signup_mx) as fecha,
	count(user_id) as completed_signup
	from klar.cck_funnel cf2
	where ended_web_signup_mx notnull
	group by fecha
	order by fecha asc
),
	sms_confirm as (
	select date_trunc('month', sms_confirmed_mx) as fecha,
	count(user_id) as sms_confirmed
	from klar.cck_funnel cf3
	where sms_confirmed_mx notnull
	group by fecha
	order by fecha asc
),
	fta as (
	select date_trunc('month', least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx)) as fecha,
	count(user_id) as fta
	from klar.cck_funnel cf4
	group by fecha
	order by fecha asc
),
	First_Credit_Eligible as (
	select date_trunc('month', first_cck_line_timestamp_mx) as fecha,
	count(user_id) as first_credit
	from klar.cck_funnel
	where migrated_user = 0
	and first_cck_line_timestamp_mx notnull
	and saca_risk_band_timestamp_mx notnull
	group by fecha
	order by fecha asc
),
fta_credit as (
	with ftas as (
	select date_trunc('month', least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx)) as fecha,
	-- Credit SMS Flag
   case when (first_cck_line_timestamp_mx notnull
        and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
        and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms,
	user_id as fta_c
	from klar.cck_funnel
	where first_cck_line_timestamp_mx notnull
	and fecha >= '2022-10-01'
	and credit_sms = 1
	order by fecha asc
	)
	select fecha,
			count(fta_c) as ftas_cred
	from ftas
	group by fecha
	order by fecha asc
),
ftas_debit as (
	with ftas as (
          select date_trunc('month', first_deposit_mx) as fecha,
          -- Credit SMS Flag
   case when (first_cck_line_timestamp_mx notnull
        and not (datediff(day, saca_risk_band_timestamp_mx, first_cck_line_timestamp_mx) > 5
        and origination_campaign like '%Foundational Test _K%')) then 1 else 0 end as credit_sms,
                    user_id as debit_ftas
             from klar.cck_funnel
          where first_deposit_mx notnull
          and first_cck_line_timestamp_mx is null
          and credit_sms = 0
          )
   select fecha,
   		count(debit_ftas) as fta_deb
   	from ftas
   	group by 1
   	order by 1
)
select started_signup.fecha as fecha, started_signup.signups_started as signups_started,
		completed_signup.completed_signup as signups_completed, sms_confirm.sms_confirmed as sms_confirmed,
		fta.fta as ftas, First_Credit_Eligible.first_credit as first_credit_eligible, fta_credit.ftas_cred as fta_from_credit,
		ftas_debit.fta_deb as debit_fta
from started_signup
left join completed_signup on completed_signup.fecha = started_signup.fecha
left join sms_confirm on sms_confirm.fecha = started_signup.fecha
left join fta on fta.fecha = started_signup.fecha
left join First_Credit_Eligible on First_Credit_Eligible.fecha = started_signup.fecha
left join fta_credit on fta_credit.fecha = started_signup.fecha
left join ftas_debit on ftas_debit.fecha = started_signup.fecha
where started_signup.fecha > '2021-03-01'
and started_signup.fecha < date_trunc('month', current_date)
order by started_signup.fecha asc;
--