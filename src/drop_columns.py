import pandas as pd, argparse, sys

parser = argparse.ArgumentParser(description='Drop one or more columns from a CSV dataset.',
								epilog = 'e.g. drop_columns.py -i input.csv -o output.csv -c "delete this" "and this"')
parser.add_argument('-c', '--columns', nargs='+',
					help = 'The columns to drop, quoted in double quotes if necessary.')
parser.add_argument('-i', '--infile',  required=True, type=argparse.FileType('r'),
					help = 'An input CSV file.')
parser.add_argument('-o', '--outfile', nargs='?', type=argparse.FileType('w'),
					help = 'An output CSV file. Defaults to stdout.',
					default=sys.stdout)
					
args = parser.parse_args()

csv = pd.DataFrame(pd.read_csv(args.infile))

missing_cols = set(args.columns) - set(list(csv))
if len(missing_cols) > 0:
	print('CSV file is missing columns!')
	print('Missing columns:')
	print(missing_cols)
	print('Available columns:')
	print(list(csv))
else:
	csv.drop(args.columns, axis = 1, inplace = True)
	csv.to_csv(args.outfile, index = False)
