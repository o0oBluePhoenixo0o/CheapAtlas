import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def _left(s, amount):
    return s[:amount]

def _right(s, amount):
    return s[-amount:]

def _mid(s, offset, amount):
    return s[offset:offset+amount]

def _swap_2_cols(df: pd.DataFrame, a:str, b:str):
    'Swap 2 columns a <-> b in a dataframe'
    
    cols = list(df.columns)
    a, b = cols.index(a), cols.index(b)
    cols[b], cols[a] = cols[a], cols[b]
    df = df[cols]
    
    return df

def show_values_on_bars(axs, h_v="v", space=0.4):
    def _show_on_single_plot(ax):
        if h_v == "v":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() / 2
                _y = p.get_y() + p.get_height()
                value = int(p.get_height())
                ax.text(_x, _y, value, ha="center") 
        elif h_v == "h":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() + float(space)
                _y = p.get_y() + p.get_height()
                value = int(p.get_width())
                ax.text(_x, _y, value, ha="left")

    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _show_on_single_plot(ax)
    else:
        _show_on_single_plot(axs)

