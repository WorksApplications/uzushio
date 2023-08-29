# Overlap ratio calculation

## Goal of the script
To calculate the overlap ratio between multiple Common Crawl dumps.
We define 3 types of overlap ratio:
- `len(dump_1 & dump_2) / len(dump_1)`
- `len(dump_1 & dump_2) / len(dump_2)`
- `len(dump_1 & dump_2) / len(dump_1 | dump_2)`

## How to run the script
If you add some new dumps and run again, the program will automatically skip the dump pairs that were already calculated.
```
python3 cal_overlap.py --dump_direc_path $dump_direc_path --output_path $output_path
```

## How to visualize the results
Use the script to process the outputed csv into a heat map figure.
```
python3 visualize.py --input_path $path_to/overlap.csv --output_path $output_path
```