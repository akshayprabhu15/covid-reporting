import matplotlib.pyplot as plt
import pandas as pd
import pandas as pd
from numpy import nan
from matplotlib import dates as mpl_dates
import mplcursors 

df = pd.read_excel('C://Users/admin/Desktop/Sentiment/pro_model.xlsx')

df.loc[df['Actuals'] == 0, ['Actuals']] = nan
df.loc[df['Actuals'] > 0, ['Predicted_Lower']] = nan
df.loc[df['Actuals'] > 0, ['Predicted_Upper']] = nan

# gca stands for 'get current axis'

fig, ax = plt.subplots()
ax = plt.gca()
y1 = df['Predicted_Lower']
y2 = df['Predicted_Upper']
x = df['Date']


date_format = mpl_dates.DateFormatter('%Y-%m-%d')
plt.gca().xaxis.set_major_formatter(date_format)

ax.fill_between(x,y1, y2, facecolor="#CC6666", alpha=0.7)

df.plot(kind='line',x='Date',y='Predicted', color='yellow', ax=ax)
df.plot(kind='line',x='Date',y='Actuals', color='green', ax=ax)
df.plot(kind='line',x='Date',y='Predicted_Lower',color='white',ax=ax)
df.plot(kind='line',x='Date',y='Predicted_Upper',color='white', ax=ax)


plt.xticks(rotation=45)
plt.legend(['Predicted','Actuals'])
plt.xlabel('Date')
mplcursors.cursor(hover=True)
# time to see our work!!
plt.show()


