import pandas as pd

def _left(s, amount):
    return s[:amount]

def _right(s, amount):
    return s[-amount:]

def _mid(s, offset, amount):
    return s[offset:offset+amount]

def _calculate_surface_area(polygon):
    'Calculate surface area of the building object'
    if polygon == None:
        area = 0
    else:
        area = polygon.area
    return area

def _swap_2_cols(df: pd.DataFrame, a: str, b: str):
    'Swap 2 columns a <-> b in a dataframe'

    cols = list(df.columns)
    a, b = cols.index(a), cols.index(b)
    cols[b], cols[a] = cols[a], cols[b]
    df = df[cols]

    return df