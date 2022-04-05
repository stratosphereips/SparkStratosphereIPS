import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.dates as dates

format_date = '%m-%d-%H'
# rejected_onepckt.csv
# rejected_any_numpckts.csv

may_ = '../feb-may/'
one_packet = '%srejected_onepckt.csv' % may_
two_packets = '%srejected_twopckts.csv' % may_
any_packets = '%srejected_any_numpckts.csv' % may_
established_to = '%sestablished_to.csv' % may_
failed_from_network = '%sfailed_from.csv' % may_
failed_from_to_network = '%sfrom_to_network.csv' % may_
established_from_to_network = '%sestablished_from_to.csv' % may_
established_to_network_28000 = '%sestablished_to_28000.csv' % may_
established_from_to_network_28000 = '%sestablished_from_to_28000.csv' % may_
established_from_network_28000 = '%sestablished_from_28000.csv' % may_

df = pd.read_csv(established_to_network_28000, names=['month', 'day', 'hour', 'Amount'],
                 dtype={'month': object, 'day': object, 'hour': object}, index_col=None)

# df['Date'] = df['Date'].apply(lambda value: datetime.strptime(value, format_date))
df['Date'] = df.apply(lambda row: datetime.strptime('-'.join(row.values.tolist()[:-1]), format_date), axis=1)

fig, ax = plt.subplots()
ax.plot_date(df.Date, df.Amount, ms=2)
ax.xaxis.set_minor_locator(dates.WeekdayLocator(byweekday=1, interval=1))
ax.xaxis.set_minor_formatter(dates.DateFormatter('%d\n%a'))
ax.xaxis.grid(True, which="minor")
ax.yaxis.grid()
ax.xaxis.set_major_locator(dates.MonthLocator())
ax.xaxis.set_major_formatter(dates.DateFormatter('\n\n\n%b'))
plt.tight_layout()
plt.show()
