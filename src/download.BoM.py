import datetime, glob, time, urllib.request, csv, pandas, os, re, argparse
from dateutil.relativedelta import relativedelta




def readBatch(inputFilename):
	print ('Reading URL batch')
	with open(inputFilename, 'r') as csvfile:
		csvreader = csv.DictReader(csvfile, delimiter=',')
		for line in csvreader:
			yield line

def downloadBatch(URLTemplateList, dryrun):
	today = pandas.datetime.today()

	for line in URLTemplateList:
		if line['Ignore?'] == 'Y':
			continue
		print ('Processing site ' + line['Site name'])
		maxperiod = re.match('([0-9]+)([A-Za-z]+)', line['Max history'])
		startdate = pandas.datetime.today() - relativedelta(days=int(maxperiod.group(1)))
		if maxperiod.group(2) == 'm':
			startdate = today - relativedelta(months=int(maxperiod.group(1)))

		ndays = (today - startdate).days + 1
		daterange = pandas.date_range(startdate, periods=ndays).tolist()

		completeURLs = set()
		for date in daterange:
			URL = date.strftime(line['URL template'])
			if not URL in completeURLs:
				completeURLs.add(URL)
				if line['File granularity'] == 'Day' or line['File granularity'] == 'Month' and date.day == 1:
					output_file = date.strftime(line['Outfile template'])
					directory = os.path.dirname(output_file)
					if not os.path.exists(directory):
						os.makedirs(directory)
					if not os.path.isfile(output_file) or line['Overwrite Policy'] == 'Refresh current month' and (today.day < 3 and (today - date).days < 31 or today.month == date.month and today.year == date.year):
						print (URL)
						if os.path.isfile(output_file):
							logfile.write("O,")
						else:
							logfile.write("N,")
						logfile.write(output_file + "," + URL + "\n")
						if not dryrun:
							try:
								urllib.request.urlretrieve(URL, output_file)
								time.sleep(3)
							except:
								logfile.write('Error: %s', sys.exc_info()[0])

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Download daily BoM data.')
	parser.add_argument('-url_template', default = 'url list.csv',
						help='Target URL patterns and destination file templates')

	args = parser.parse_args()
	print ('Starting BoM downloader')
	logfile = open("download log.txt", "w+")
	URLTemplateList = readBatch(args.url_template)
	downloadBatch(URLTemplateList, False)
	print ('Finished.')

