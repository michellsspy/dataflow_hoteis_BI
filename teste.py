from datetime import datetime
data_now = datetime.now()
data_now = str(data_now).replace(' ', '_').replace(':', '.')
print(data_now)