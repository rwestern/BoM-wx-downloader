# Script to post-process downloaded BoM files into a set of reformatted CSV files
# R Western 15/12/2016

import tarfile, datetime, os, fnmatch, fileinput, re, collections, argparse

dailyfileheader = '\"Date\",\"Minimum temperature (C)\",\"Maximum temperature (C)\",\"Rainfall (mm)\",\"Evaporation (mm)\",\"Sunshine (hours)\",\"Direction of maximum wind gust \",\"Speed of maximum wind gust (km/h)\",\"Time of maximum wind gust\",\"9am Temperature (C)\",\"9am relative humidity (%)\",\"9am cloud amount (oktas)\",\"9am wind direction\",\"9am wind speed (km/h)\",\"9am MSL pressure (hPa)\",\"3pm Temperature (C)\",\"3pm relative humidity (%)\",\"3pm cloud amount (oktas)\",\"3pm wind direction\",\"3pm wind speed (km/h)\",\"3pm MSL pressure (hPa)\",\"BoM Station ID\",\"File\",\"Location name\",\"Period\"'
hourlyfileheader = 'WMO,name,history_product,time_zone_name,TDZ,aifstime_utc,aifstime_local,lat,lon,apparent_t,cloud,cloud_base_m,cloud_oktas,cloud_type_id,cloud_type,delta_t,gust_kmh,gust_kt,air_temp,dewpt,press,press_msl,press_qnh,press_tend,rain_hour,rain_ten,rain_trace,rain_trace_time,rain_trace_time_utc,local_9am_date_time,local_9am_date_time_utc,duration_from_local_9am_date,rel_hum,sea_state,swell_dir_worded,swell_height,swell_period,vis_km,weather,wind_dir,wind_dir_deg,wind_spd_kmh,wind_spd_kt,wind_src,File'

def processDaily(filelist):
	print('Processing daily data...')
	
	outputstream.write(dailyfileheader + '\n')
	
	writtenlines = set()
	
	headerregex = re.compile('^(?!,2)')
	stationidregex = re.compile('\{station 0([0-9]{5})\}')
	nullsregex = re.compile('(?:,\"\"|,\" \"|, )(?=,|$)')
	for line in fileinput.input(filelist):
		if fileinput.isfirstline():
			filename = os.path.basename(fileinput.filename())
			print (filename)
			headerline = line
			fileinfo = re.search('Daily Weather Observations for ([\'A-Za-z ()\-]+)(?:, Victoria)? for ([A-Z][a-z]+ [0-9]{4})', headerline)
			if fileinfo is None:
				fileinfo = headerline.rstrip() + ','
			else:
				fileinfo = '\"' + fileinfo.group(1) + '\",\"' + fileinfo.group(2) + '\"'
			suffix = ',\"' + filename + '\",' + fileinfo
			stationid = ''
			
		matchkey = (line[1:11], stationid) # a tuple of (date, station id)
		outline = line.rstrip() + ',' + stationid + suffix + '\n'
		if headerregex.search(line) is None and matchkey not in writtenlines:
			# not a header line, save to disk
			if outline[0] == ',':
				outline = outline[1:] # drop leading comma
			if nullsregex.search(outline):
				outline = nullsregex.sub(',', outline) # strip out blanks
			writtenlines.add(matchkey)
			yield outline
		elif stationidregex.search(line) and stationid == '':
			# header line, scan for a station id
			stationid = stationidregex.search(line).group(1)
	
def sectionedHourlyData(inputstream):
	# Gathers input data into a dict arranged by section: notes, header, data, etc.
	sectionregex = re.compile('^\[([a-z]+)\]')

	output = dict()
	for line in inputstream:
		line = line.rstrip()
		if sectionregex.match(line):
			sectionname = sectionregex.match(line).group(1)
			output[sectionname] = collections.deque()
		elif line != '[$]' and line != '' and not line.startswith('sort_order'):
			output[sectionname].append(line)
			
	#headerregex = re.compile('([A-Za-z_]+)\[[0-9]+\]=\"(.*)\"')
	#output['header'] = dict(re.search('([A-Za-z_]+)\[[0-9]+\]=\"(.*)\"', item).groups() for item in output['header'])
	return (output)
	
def processHourly(filelist):
	print('Processing hourly data...')
	outputstream.write(hourlyfileheader + '\n')
	
	writtenlines = set()
	headerregex = re.compile('^\[([a-z]+)\]')
	nullsregex = re.compile('(?:,-9999|,-9999.0|,-|,\"-\"|,\"\")(?=,|$)')
	keyregex = re.compile('^([0-9]{5,6})(?:,\"[^\"]+\"){4},\"([^\"]+)\",')
	for filename in filelist:
		print(os.path.basename(filename))
		sectioneddata = sectionedHourlyData(open(filename, 'r'))
		for item in sectioneddata['data']:
			item = item.split(',', 1)[1] # drop first column, sort_order
			matchkey = keyregex.search(item).groups() # a tuple of (WMO ID, timestamp)
			if matchkey not in writtenlines: # only save it if it's not a duplicate.
				writtenlines.add(matchkey)
				if nullsregex.search(item):
					item = nullsregex.sub(',', item) # strip out representations of null like -9999 or -
				yield item + ',\"' + os.path.basename(filename) + '\"' + '\n'

def filterTarContents(tar, pattern, targetpath):
		for tarinfo in tar:
			if fnmatch.fnmatch(tarinfo.name, pattern) and not os.path.isfile(os.path.join(targetpath, tarinfo.name)):
				yield tarinfo
	
def unTarHourlyFiles(filelist):
	print('Untarring archives...')
	for fname in filelist:
		print('\t' + fname)
		tar = tarfile.open(fname)
		targetpath = os.path.splitext(fname)[0]
		if not os.path.exists(targetpath):
			os.makedirs(targetpath)
		tar.extractall(members = filterTarContents(tar, '*.axf', targetpath), path = targetpath)
		tar.close()
		
def gatherFilelist(targetfolder, pattern):
	for dirpath, dirnames, files in os.walk(targetfolder):
		for f in fnmatch.filter(files, pattern):
			yield os.path.join(dirpath, f)
	
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Process and concatenate daily/hourly BoM data.')
	parser.add_argument('-daily_folder', default = './by day/',
						help='The source folder for the BoM daily data')
	parser.add_argument('-hourly_folder', default = './by hour/',
						help='The source folder for the BoM hourly data')
	parser.add_argument('-hourly_file', default='./hourly.BoM.csv',
						help='The output filename for the hourly dataset.')
	parser.add_argument('-daily_file', default='./daily.BoM.csv',
						help='The output filename for the daily dataset.')
	args = parser.parse_args()

	unTarHourlyFiles(gatherFilelist(args.hourly_folder, '*.tgz'))
	
	with open(args.hourly_file, 'w') as outputstream:
		outputstream.writelines(processHourly(gatherFilelist(args.hourly_folder, '*IDV*.*.axf')))
	
	with open(args.daily_file, 'w') as outputstream:
		outputstream.writelines(processDaily(gatherFilelist(args.daily_folder, 'IDCJDW*.csv')))
	
	print ('Finished.')
	
