#!env python3

import random
import string

random.seed(42)

tables = ["R", "S", "T"]
schema = ["id", "fkey", "rfloat", "rstring"]
types  = ["int", "int", "float", "string"]
n_tuples = 100
str_len = 15

for tab in tables:
    with open("test/end2end/data/ours/" + tab + ".csv", 'w') as file:
        # write out header
        for (i, attr) in enumerate(schema):
            if i > 0:
                file.write(",")
            file.write(f"{attr}")
        file.write('\n')

        # write tuples
        for i in range(n_tuples):
            file.write(str(i))  # write id

            for(j, attr) in enumerate(schema):
                if j == 0:
                    continue  # id is written in outer loop
                ty = types[j]
                if ty == "int":
                    val = random.randint(0, n_tuples - 1)
                elif ty == "float":
                    val = float('%.5f'%(random.uniform(0, 10)))
                elif ty == "string":
                    val = '\"' + ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits + ' ', k=str_len)) + '\"'
                file.write(',' + str(val))
            file.write('\n')
