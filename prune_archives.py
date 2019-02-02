import pandas as pd
import datetime
import boto3
import time

g = boto3.client('glacier')

df = pd.read_csv('glacier_archive_list.csv', parse_dates=['timestamp_deleted', 'timestamp_uploaded'])
df['timestamp_deleted'] = df['timestamp_deleted'].dt.tz_localize('UTC')
filter_mask = ((pd.to_datetime(pd.datetime.utcnow()).tz_localize('UTC') - df['timestamp_uploaded']) > datetime.timedelta(days=90)) & (df['type'] != 'photos')

delete_frame = df[filter_mask].iterrows()
pause_time = 10
print("Pausing {} s before deleting the following {} archives: {}".format(pause_time, delete_frame.shape[1], delete_frame))
time.sleep(pause_time)

for idx, row in df[filter_mask].iterrows():
    print("Deleting {} with upload date of {} from vault {}".format(row['source_dir'], row['timestamp_uploaded'], row['vault_name']))
    print(g.delete_archive(vaultName=row['vault_name'], archiveId=row['archive_id']))

df.loc[filter_mask, 'timestamp_deleted'] = datetime.datetime.utcnow()
# make sure it stays localized
df['timestamp_deleted'] = df['timestamp_deleted'].dt.tz_localize('UTC')

print('Writing updated archive list to file')
df.to_csv('glacier_archive_list.csv', index=None, date_format='%Y-%m-%dT%H:%M:%SZ')

