import sys

import pandas as pd

path =''
df_nifi = pd.read_csv(sys.stdin, header=0, delimiter=';')
df_nifi[sys.argv[1]] = df_nifi[sys.argv[1]].str.split('|')
df = df_nifi.explode(sys.argv[1])
df = df.loc[(df[sys.argv[1]].astype(int) > 2000)]
df.to_csv(sys.stdout, index=False)
