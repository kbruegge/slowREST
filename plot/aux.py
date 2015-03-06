__author__ = 'kai'
import pygal
from pygal.style import LightSolarizedStyle


def test_plot():
    bar_chart = pygal.HorizontalStackedBar(style=LightSolarizedStyle)
    bar_chart.title = "Remarquable sequences"
    bar_chart.x_labels = map(str, range(11))
    bar_chart.add('Fibonacci', [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55])
    bar_chart.add('Padovan', [1, 1, 1, 2, 2, 3, 4, 5, 7, 9, 12])
    chart = bar_chart.render(is_unicode=True)
    return chart
