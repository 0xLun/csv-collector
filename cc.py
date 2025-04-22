import csv
import json
import argparse
import os
import re
import sys


def log(message, level="info", verbosity=0, min_level=1):
    levels = {"error": "[ERROR]", "warn": "[WARNING]", "info": "[INFO]", "debug": "[DEBUG]"}
    if verbosity >= min_level:
        print(f"{levels[level]} {message}", file=sys.stderr if level == "error" else sys.stdout)

def load_config(config_path, verbosity):
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            log(f"Loading config file: {config_path}", "debug", verbosity, 2)
            return json.load(f)
    except Exception as e:
        log(f"Unable to load config file '{config_path}': {e}", "error")
        sys.exit(1)

def compute_output_fields(config, sample_row):
    base_fields = list(sample_row.keys())
    added_fields = []

    for field in config.get("add-fields", []):
        name = field["name"]
        after = field.get("after")
        default = field.get("default-value")

        if name in base_fields:
            continue

        insert_index = len(base_fields)
        if after and after in base_fields:
            insert_index = base_fields.index(after) + 1

        base_fields.insert(insert_index, name)
        added_fields.append((name, default))

    for name, default in added_fields:
        sample_row[name] = default

    output_fields = config.get("output", {}).get("fields")
    if not output_fields:
        output_fields = list(sample_row.keys())
        
        rule_field = config["output"].get("rule-match-field", "_rule")
        file_field = config["output"].get("file-processed-field", "_file")
        
        if rule_field not in output_fields:
            output_fields.insert(0, rule_field)
        if file_field not in output_fields:
            output_fields.insert(1, file_field)
   
    
    config["output"]["computed_fields"] = output_fields
    return output_fields

def apply_match_assertions(row, rule, regex_flags):
    match_summary = []
    assertions =  rule.get("match", [])
    for assertion in assertions:
        for field in assertion.get("fields", []):
            if field in row:
                value = row[field]
                match_obj = re.search(assertion["regex"], value, flags=regex_flags)
                if match_obj:
                    match_summary.append(f"{field}:{match_obj.group(0)}")
                    break
                    
    return len(match_summary)>=len(assertions), match_summary


def apply_write_truth(row, rule, match_summary):
    write_truth = rule.get("write-truth")
    if write_truth:
        target_field = write_truth.get("field")
        value = write_truth.get("value", "")
        if "$match" in value.lower():
            summary_str = " AND ".join(match_summary)
            value = value.replace("$match", summary_str)
        row[target_field] = value


def process_row(config, row, file_path, row_number, computed_fields, verbosity):
    rule_field = config["output"].get("rule-match-field", "_rule")
    if rule_field in computed_fields and rule_field not in row : row[rule_field] = ""
    processed_rows = []
    no_rule_matched = True

    # 🛠 Apply default values to missing fields
    for field_def in config.get("add-fields", []):
        name = field_def["name"]
        default = field_def.get("default-value", "")
        if name not in row or row[name] == "":
            row[name] = default

    for rule in config.get("rules", []):
        regex_flags = 0 if rule.get("case-sensitive", False) else re.IGNORECASE
        rule_name = rule.get("name", "no-name")
        action = rule.get("action")
        write_truth = rule.get("write-truth")
        log(f"Processing rule {rule_name} with '{action}' action", "debug", verbosity, 4)

        matched, match_summary = apply_match_assertions(row, rule, regex_flags)

        if not matched:
            continue
        
        log(f"Match rule {rule['name']} in {file_path} at row {row_number} ", "info", verbosity, 2)
        no_rule_matched = False

        if action == "drop-row":
            log(f"Dropping row {row_number} due to rule '{rule_name}'", "info", verbosity, 2)
            return []

        elif action == "replace":
            for assertion in rule.get("match", []):
                for field in assertion.get("fields", []):
                    regex = assertion.get("regex")
                    if field in row:
                        row[field] = re.sub(regex, rule.get("replace-by", ""), row[field], flags=regex_flags)
            if write_truth : apply_write_truth(row, rule, match_summary)

        elif action == "create-row":
            row_copy = row.copy()
            if write_truth : apply_write_truth(row_copy, rule, match_summary)
            if rule_field in computed_fields : row_copy[rule_field] = rule_name
            processed_rows.append(row_copy)

        elif action == "pipe":
            if write_truth : apply_write_truth(row, rule, match_summary)
            if rule_field in computed_fields :
                if row[rule_field]:
                    row[rule_field] = " | ".join([row[rule_field], rule_name])
                else:
                    row[rule_field] = rule_name

    if no_rule_matched:
        if config["output"].get("drop-unmatched"):
            log(f"Dropping row {row_number} due to no match", "info", verbosity, 4)
            return []
        log(f"Unprocessed row {row_number} returned despite no match", "info", verbosity, 4)
        return [row]

    if not processed_rows:
        log(f"Unprocessed row {row_number} returned", "debug", verbosity, 4)
        return [row]

    log(f"Processed row {row_number} returned", "debug", verbosity, 4)
    return processed_rows


def process_csv(file_path, config, writer, verbosity):
    file_field = config["output"].get("file-processed-field", "_file")
    computed_fields = None
    try:
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)

            if not reader.fieldnames:
                log(f"Skipping file '{file_path}' - no headers found.", "warn", verbosity, 1)
                return

            log(f"Processing file: {file_path}", "info", verbosity, 1)

            for row_number, row in enumerate(reader, start=1):
                if not row:
                    log(f"Skipping empty row {row_number} in '{file_path}'", "warn", verbosity, 3)
                    continue

                if "computed_fields" not in config["output"]:
                    computed_fields = compute_output_fields(config, row.copy())
                    writer.fieldnames = computed_fields
                    writer.writeheader()
                else :
                    computed_fields = config["output"]["computed_fields"]

                log(f"Processing row {row_number} in '{file_path}'", "debug", verbosity, 3)
                matched_rows = process_row(config, row, file_path, row_number,computed_fields, verbosity)

                for matched_row in matched_rows:
                    if file_field in computed_fields : matched_row[file_field] = os.path.basename(file_path)
                    output_row = {key: matched_row.get(key, "") for key in writer.fieldnames}
                    writer.writerow(output_row)

    except Exception as e:
        log(f"Unable to process file '{file_path}': {e}", "error")


def process_directory(input_path, config, writer, verbosity):
    if not os.path.exists(input_path):
        log(f"The input directory '{input_path}' does not exist.", "error")
        sys.exit(1)

    csv_files = [f for f in os.listdir(input_path) if f.endswith(".csv")]
    if not csv_files:
        log(f"No CSV files found in directory '{input_path}'.", "warn", verbosity, 1)
        return

    for file_name in csv_files:
        file_path = os.path.join(input_path, file_name)
        process_csv(file_path, config, writer, verbosity)

def main():
    parser = argparse.ArgumentParser(description="Parse CSV files based on regex rules")
    parser.add_argument("-i", "--input", required=True, help="Input CSV file or directory")
    parser.add_argument("-o", "--output", required=True, help="Output CSV file")
    parser.add_argument("-c", "--config", required=True, help="Path to config.json file")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity level (-v for info, -vv for debug)")

    args = parser.parse_args()
    config = load_config(args.config, args.verbose)

    try:
        with open(args.output, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[])  # fieldnames updated in process_csv
            if os.path.isdir(args.input):
                process_directory(args.input, config, writer, args.verbose)
            else:
                process_csv(args.input, config, writer, args.verbose)

    except Exception as e:
        log(f"Unable to write to output file '{args.output}': {e}", "error")
        sys.exit(1)
    
    log(f"Processing end", "info", args.verbose, 4)
    


if __name__ == "__main__":
    main()
