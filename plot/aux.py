__author__ = 'kai'
import pygal
from pygal.style import LightSolarizedStyle
# import numpy as np
# from fact import time


def create_box_whisker(data_frame):
    try:
        #prepare and clean data. drop missings as well
        data_frame.drop(["_id"], axis=1, inplace='true')
        data_frame.set_index("Time", inplace='true')
        data_frame = data_frame[data_frame.applymap(lambda x: isinstance(x, (int, float)))]
        data_frame.dropna(axis=1, inplace='true')
    except ValueError:
        pass
    except:
        return None

    bar_chart = pygal.Box(style=LightSolarizedStyle, logarithmic=False)
    bar_chart.title = "Overview"
    # print(data_frame)
    for attribute in data_frame.columns.values:
        bar_chart.add(attribute, list(data_frame[attribute]))

    return bar_chart.render()


# def create_plots(data_frame, bins=12):
#     # print(data_frame.columns.values)
#     # print(data_frame["_id"])
#     try:
#         data_frame = data_frame.drop(["_id"], axis=1)
#     except ValueError:
#         pass
#
#     # print(data_frame)
#     bin_width = len(data_frame)/bins
#     try:
#         binned = data_frame.groupby(data_frame.index // int(bin_width)).mean()
#     except:
#         return []
#
#     dates = list(map(time.to_datetime, binned.Time))
#
#     binned = binned.set_index("Time")
#     #print(binned)
#     #print(str(dates))
#     charts = []
#     for attribute in binned.columns.values:
#         bar_chart = pygal.Bar(style=LightSolarizedStyle, x_label_rotation=33,
#                               show_legend=False, human_readable=True, fill=False, x_scale=.45, y_scale=.45)
#         bar_chart.title = attribute
#         bar_chart.x_labels = map(str, dates)
#         bar_chart.add(attribute, list(binned[attribute]))
#         # chart = bar_chart.render(is_unicode=True)
#         charts.append(bar_chart.render(is_unicode=True))
#
#     return charts
