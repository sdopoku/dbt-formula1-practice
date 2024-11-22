with results as (

    select * from {{ ref('stg_formula1__results') }}

),

races as (

    select * from {{ ref('stg_formula1__races') }}

),

drivers as (

    select * from {{ ref('stg_formula1__drivers') }}

),

constructors as (

    select * from {{ ref('stg_formula1__constructors') }}
),

status as (

    select * from {{ ref('stg_formula1__status') }}
),

int_results as (
    select
        result_id,
        results.race_id,
        race_year,
        race_round,
        circuit_id,
        circuit_name,
        race_date,
        race_time,
        results.driver_id,
        results.driver_number,
        cast(datediff('year', date_of_birth, race_date) as int)
            as drivers_age_years,
        driver_nationality,
        results.constructor_id,
        constructor_name,
        constructor_nationality,
        grid,
        position,
        position_text,
        position_order,
        points,
        laps,
        results_time_formatted,
        results_milliseconds,
        fastest_lap,
        results_rank,
        fastest_lap_time_formatted,
        fastest_lap_speed,
        results.status_id,
        status,
        forename || ' ' || surname as driver,
        case when position is null then 1 else 0 end as dnf_flag
    from results
    left join races
        on results.race_id = races.race_id
    left join drivers
        on results.driver_id = drivers.driver_id
    left join constructors
        on results.constructor_id = constructors.constructor_id
    left join status
        on results.status_id = status.status_id
)

select * from int_results
