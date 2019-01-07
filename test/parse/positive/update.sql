-- Blind update.
UPDATE mytable
SET
    x = 42,
    y = 3.14,
    vc = "Hello, World",
    b = TRUE;

-- Update with condition.
UPDATE mytable
SET
    x = 42,
    y = 3.14,
    vc = "Hello, World",
    b = TRUE
WHERE
    (z < 1337);
