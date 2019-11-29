#!env python3

import psycopg2

conn = psycopg2.connect('dbname=reference user=postgres')
cur = conn.cursor()

queries = [
    # SELECT all datatypes
    ('SELECT 42;', 'select_int'),
    ('SELECT 13.37;', 'select_double'),
    ('SELECT "Hello, world!";', 'select_string'),
    ('SELECT TRUE;', 'select_boolean'),
    ('SELECT NULL;', 'select_null'),
    ('SELECT 42, 13.37, "Hello, wolrd!", TRUE, NULL;', 'select_types_mixed'),
    ('SELECT 42 + 13;', 'select_expr_const'),
    # SELECT FROM
    ('SELECT * FROM R ORDER BY key;', 'select_all'),
    ('SELECT key FROM R ORDER BY key;', 'select_attr_1'),
    ('SELECT fkey, key FROM R ORDER BY key;', 'select_attr_2'),
    ('SELECT R.key FROM R ORDER BY R.key;', 'select_attr_with_prefix'),
    ('SELECT * FROM R, S ORDER BY R.key, S.key;', 'select_all_from_cartesian_prod'),
    ('SELECT *, key, 42 FROM R ORDER BY key;', 'select_anti'),
    ('SELECT key, fkey - key FROM R ORDER BY key;', 'select_expr_attr'),
    ('SELECT 42, 42, "abc", "abc", key, key FROM R ORDER BY key;', 'select_repeatedly'),
    # WHERE
    ('SELECT * FROM R WHERE key = 42;', 'where_equal'),
    ('SELECT * FROM R WHERE key < 42 ORDER BY key;', 'where_less'),
    ('SELECT * FROM R WHERE key > 15 AND key < 30 AND key < 17 ORDER BY key;' , 'where_conjunction'),
    ('SELECT * FROM R WHERE key > 15 OR key < 17 OR key < 70 ORDER BY key;' , 'where_disjunction'),
    ('SELECT * FROM R WHERE TRUE ORDER BY key;', 'where_true'),
    ('SELECT * FROM R WHERE FALSE;', 'where_false'),
    ('SELECT * FROM R WHERE rstring < "m" ORDER BY key;', 'where_strcmp'),
    # GROUP BY
    ('SELECT fkey FROM R GROUP BY fkey ORDER BY fkey;', 'groupby_attr'),
    ('SELECT key FROM R GROUP BY key ORDER BY key;', 'groupby_primary_key'),
    ('SELECT key, fkey FROM R GROUP BY key, fkey ORDER BY key;', 'groupby_compound_key'),
    ('SELECT key + fkey FROM R GROUP BY key + fkey ORDER BY key + fkey;', 'groupby_expr'),
    ('SELECT fkey, COUNT(key) FROM R GROUP BY fkey ORDER BY fkey;', 'groupby_with_aggregation_count'),
    ('SELECT COUNT(fkey) FROM R;', 'aggregation_without_groupby'),
    # HAVING
    ('SELECT 42 FROM R HAVING SUM(key) > 100; SELECT 13 FROM R HAVING SUM(key) < 100;', 'having_without_groupby'),
    ('SELECT 42, COUNT(key) FROM R HAVING SUM(key) > 100;', 'having_with_aggregation_without_groupby'),
    ('SELECT fkey FROM R GROUP BY fkey HAVING COUNT(key) > 1 ORDER BY fkey;', 'having_with_groupby'),
    # ORDER BY
    ('SELECT * FROM R ORDER BY key ASC;', 'orderby_attr_asc'),
    ('SELECT * FROM R ORDER BY key DESC;', 'orderby_attr_desc'),
    ('SELECT key + fkey FROM R ORDER BY key + fkey;', 'orderby_expression'),
    ('SELECT * FROM R ORDER BY fkey, key;', 'orderby_compound'),
    # LIMIT
    ('SELECT * FROM R ORDER BY key LIMIT 3;', 'limit'),
    ('SELECT * FROM R ORDER BY key LIMIT 3 OFFSET 4;', 'limit_with_offset'),
    # JOINS
    ('SELECT R.key, S.key FROM R, S WHERE R.key = S.fkey AND R.key < 10 ORDER BY R.key, S.key;', 'join_binary'),
    ('SELECT R.key, S.key, T.key FROM R, S, T WHERE R.key = S.fkey + T.fkey AND R.key < 10 ORDER BY R.key, S.key, T.key;', 'join_ternary'),
    ('SELECT R.key, S.key, T.key FROM R, S, T WHERE R.key = S.fkey AND R.key = T.fkey AND R.key < 10 ORDER BY R.key, S.key, T.key;', 'join_binary_x2'),
    # SUBQUERY
    ('SELECT * FROM (SELECT * FROM R) AS sub ORDER BY key;', 'subquery_from_select_all'),
    ('SELECT fkey FROM (SELECT key, fkey FROM R) AS sub ORDER BY fkey;', 'subquery_from_select_attr'),
    ('SELECT sub.key FROM (SELECT key FROM R) AS sub ORDER BY sub.key;', 'subquery_from_select_attr_with_prefix'),
    ('SELECT * FROM (SELECT fkey, COUNT(key) FROM R GROUP BY fkey) AS sub ORDER BY fkey;', 'subquery_from_groupby'),
]
def stringify(x):
    if x == None:
        return 'NULL'
    elif type(x) == str:
        return '\"' + x + '\"'
    elif type(x) == bool:
        return 'TRUE' if x else 'FALSE'
    else:
        return str(x)

for (i, query) in enumerate(queries):
    sql, name = query
    with open("test/end2end/positive/queries/" + name + '.sql', 'w') as q_file:
        # write query
        q_file.write(sql + '\n')

        # write results
        with open("test/end2end/positive/queries/" + name + '.csv', 'w') as r_file:
            sql = sql.replace('"', '\'')
            res = list()
            for stmt in sql.split(';')[0:-1]:
                cur.execute(stmt + ';')
                res += cur.fetchall()
            for r in res:
                r_file.write(','.join(map(stringify, r)) + '\n')
            r_file.close()
        q_file.close()

cur.close()
conn.close()
