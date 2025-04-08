# csv-collector
Simple CSV parser that collects fields based on regular expressions

## Requirements

```
Python 3+
```

## Usage
```
python3 cc.py -i <input-folder|input-file> -o <output-file> -c <config-file> -<verbosity>
```
```
python3 cc.py -i ./files/ -o ./output.csv -c ./config.json -vv
```

## Manual 

```
usage: cc.py [-h] -i INPUT -o OUTPUT -c CONFIG [-v]

Parse CSV files based on regex rules

options:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Input CSV file or directory
  -o OUTPUT, --output OUTPUT
                        Output CSV file
  -c CONFIG, --config CONFIG
                        Path to config.json file
  -v, --verbose         Increase verbosity level (-v for info, -vv for debug)
```
