-- This is a dummy test that will always fail to test the behavior
-- It checks for a condition that should never be true

SELECT 1 as failing_condition
WHERE 1 = 2  -- This will never be true, so the test should pass
   OR 'always' = 'fail'  -- This makes it fail by returning a row