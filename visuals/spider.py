import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 

plt.style.use('ggplot')

def spider(data, name1, name2, name3= None):
    """"""

    data_name = data[(data['last_name']== name1) | (data['last_name']== name2) | (data['last_name']== name3) ]
    col_agg = {'ast':'sum', 'blk':'sum', 'dreb':'sum', 'fg3a':'sum',\
        'fg3m':'sum', 'fga':'sum', 'fgm':'sum', 'fta':'sum',\
        'ftm':'sum', 'pf':'sum', 'pts':'sum', 'reb':'sum',\
        'stl':'sum', 'turnover':'sum', 'season':'nunique',\
        'min_played':'sum', 'eFG%':'mean', 'TS%':'mean', 'PER':'mean'}
    data_ty = data_name.groupby('last_name').agg(col_agg).reset_index()
    cols = data_ty.select_dtypes(exclude = 'object').columns
    fig, ax = plt.subplots(figsize=  (12, 12), subplot_kw=dict(polar=True))
    angles = np.linspace(0, 2*np.pi, len(cols), endpoint = False).tolist()
    angles += angles[:1]
    for i, row in data_ty.iterrows():
        values = row[cols].tolist()
        values += values[:1]
        ax.plot(angles, values, 'o-', linewidth=1.0, label = row['last_name'])
        for i, c in zip([0.1, 0.15, 0.2],['orange', 'green', 'blue']):
            ax.fill(angles, values, alpha = i, color = c)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(cols)
    ax.set_title('Player Performance')
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1))
    plt.show() 