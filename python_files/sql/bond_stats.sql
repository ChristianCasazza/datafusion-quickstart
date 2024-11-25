WITH payment_stats AS (
    SELECT
        general_ledger,
        MIN(timestamp) AS first_payment_date,
        MAX(timestamp) AS last_payment_date,
        AVG(amount) AS average_payment,
        COUNT(DISTINCT timestamp) AS total_payments
    FROM
        mta_operations_statement
    WHERE
        scenario = 'Actual'
        AND type = 'Debt Service Expenses'
    GROUP BY
        general_ledger
),
first_payment AS (
    SELECT
        general_ledger,
        MIN(timestamp) AS first_payment_date,
        MIN(amount) AS first_payment_amount
    FROM
        mta_operations_statement
    WHERE
        scenario = 'Actual'
        AND type = 'Debt Service Expenses'
    GROUP BY
        general_ledger
),
last_payment AS (
    SELECT
        general_ledger,
        MAX(timestamp) AS last_payment_date,
        MAX(amount) AS last_payment_amount
    FROM
        mta_operations_statement
    WHERE
        scenario = 'Actual'
        AND type = 'Debt Service Expenses'
    GROUP BY
        general_ledger
)
SELECT DISTINCT
    ps.general_ledger,
    fp.first_payment_date,
    fp.first_payment_amount,
    ps.last_payment_date,
    lp.last_payment_amount,
    ps.average_payment,
    ps.total_payments
FROM
    payment_stats ps
LEFT JOIN
    first_payment fp ON ps.general_ledger = fp.general_ledger
LEFT JOIN
    last_payment lp ON ps.general_ledger = lp.general_ledger
ORDER BY
    ps.general_ledger;
