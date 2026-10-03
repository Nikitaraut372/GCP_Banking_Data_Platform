INSERT INTO `banking_gold.daily_account_balance`
SELECT
  account_sk,
  account_id,
  customer_id,
  balance,
  CURRENT_DATE() AS balance_date
FROM `banking_silver.account_balance`;
